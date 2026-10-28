// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Per-tenant CPU, memory, and disk-page budgets.
//!
//! A tenant is a SQL namespace. Caps live here, not on the fiber: every
//! statement of that namespace charges the same account. Zero means no
//! cap, which is how a namespace starts, so a process that never sets a
//! budget behaves as it did before.
//!
//! CPU nanoseconds and disk pages refill once a second. Memory is live
//! bytes, not a window: it falls when the same tenant frees. A free on
//! some other fiber stays charged — the allocator hook only sees who is
//! running, and it never creates an account (that allocation would
//! re-enter this map under its own lock).

const std = @import("std");
const _clock = @import("utils").coarse_clock;
const _rt = @import("runtime.zig");
const Spinlock = @import("spinlock.zig").Spinlock;

pub const Id = u64;

/// How long a CPU or disk-page window lasts. `maybeYield` parks for
/// whatever is left of it once the tenant is over.
pub const window_ms: i64 = 1000;

pub const Caps = struct {
    /// CPU nanoseconds allowed in one window. Zero does not cap.
    cpu_ns_per_s: u64 = 0,
    /// Live bytes. Zero does not cap.
    mem_bytes: u64 = 0,
    /// Disk pages allowed in one window. Zero does not cap.
    iops_per_s: u64 = 0,
};

pub const Account = struct {
    cpu_ns_per_s: std.atomic.Value(u64) = .init(0),
    mem_cap: std.atomic.Value(u64) = .init(0),
    iops_per_s: std.atomic.Value(u64) = .init(0),
    cpu_ns: std.atomic.Value(u64) = .init(0),
    mem_live: std.atomic.Value(u64) = .init(0),
    iops: std.atomic.Value(u64) = .init(0),
    window_ms: std.atomic.Value(i64) = .init(0),
    window_cpu: std.atomic.Value(u64) = .init(0),
    window_iops: std.atomic.Value(u64) = .init(0),

    fn roll(self: *Account, now: i64) void {
        const start = self.window_ms.load(.monotonic);
        if (now - start < window_ms) return;
        if (self.window_ms.cmpxchgStrong(
            start,
            now,
            .acq_rel,
            .monotonic,
        ) != null) return;
        self.window_cpu.store(0, .monotonic);
        self.window_iops.store(0, .monotonic);
    }

    fn addCpu(self: *Account, ns: u64) void {
        _ = self.cpu_ns.fetchAdd(ns, .monotonic);
        self.roll(_clock.ms());
        _ = self.window_cpu.fetchAdd(ns, .monotonic);
    }

    fn addIops(self: *Account, pages: u64) void {
        _ = self.iops.fetchAdd(pages, .monotonic);
        self.roll(_clock.ms());
        _ = self.window_iops.fetchAdd(pages, .monotonic);
    }

    fn addMem(self: *Account, delta: i64) void {
        if (delta > 0) {
            _ = self.mem_live.fetchAdd(@intCast(delta), .monotonic);
            return;
        }
        if (delta == 0) return;
        const n: u64 = @intCast(-delta);
        while (true) {
            const old = self.mem_live.load(.monotonic);
            const next = old -| n;
            if (self.mem_live.cmpxchgWeak(
                old,
                next,
                .monotonic,
                .monotonic,
            ) == null) return;
        }
    }

    pub fn memoryOver(self: *Account) bool {
        const cap = self.mem_cap.load(.monotonic);
        if (cap == 0) return false;
        return self.mem_live.load(.monotonic) > cap;
    }

    /// Milliseconds left in the window when CPU or disk pages are over
    /// their cap, else 0. The caller parks; it does not loop.
    pub fn paceWaitMs(self: *Account) u64 {
        const now = _clock.ms();
        self.roll(now);
        const cpu_cap = self.cpu_ns_per_s.load(.monotonic);
        const io_cap = self.iops_per_s.load(.monotonic);
        const cpu_over = cpu_cap != 0 and
            self.window_cpu.load(.monotonic) > cpu_cap;
        const io_over = io_cap != 0 and
            self.window_iops.load(.monotonic) > io_cap;
        if (!cpu_over and !io_over) return 0;
        const start = self.window_ms.load(.monotonic);
        const elapsed = now - start;
        if (elapsed < 0 or elapsed >= window_ms) return 0;
        return @intCast(window_ms - elapsed);
    }
};

const Snap = struct {
    id: Id,
    cpu_ns: u64,
    mem_live: u64,
    iops: u64,
    cpu_cap: u64,
    mem_cap: u64,
    iops_cap: u64,
};

var sl: Spinlock = .{};
var gpa: std.mem.Allocator = undefined;
var accounts: std.AutoHashMap(Id, *Account) = undefined;
var ready: bool = false;

/// Set while this file allocates, so the live-bytes hook does not take
/// `sl` again. The hook runs on the allocating fiber, which may already
/// hold the lock inside `ensure`.
threadlocal var in_budget: bool = false;

pub fn bind(allocator: std.mem.Allocator) void {
    if (ready) return;
    gpa = allocator;
    accounts = std.AutoHashMap(Id, *Account).init(allocator);
    ready = true;
    @import("utils").mem.setLiveHook(noteLive);
}

pub fn unbind() void {
    if (!ready) return;
    @import("utils").mem.setLiveHook(null);
    sl.lock();
    defer sl.unlock();
    ready = false;
    var it = accounts.iterator();
    while (it.next()) |entry| gpa.destroy(entry.value_ptr.*);
    accounts.deinit();
}

pub fn set(id: Id, next: Caps) !void {
    const acct = try ensure(id);
    acct.cpu_ns_per_s.store(next.cpu_ns_per_s, .monotonic);
    acct.mem_cap.store(next.mem_bytes, .monotonic);
    acct.iops_per_s.store(next.iops_per_s, .monotonic);
}

pub fn getCaps(id: Id) Caps {
    const acct = find(id) orelse return .{};
    return .{
        .cpu_ns_per_s = acct.cpu_ns_per_s.load(.monotonic),
        .mem_bytes = acct.mem_cap.load(.monotonic),
        .iops_per_s = acct.iops_per_s.load(.monotonic),
    };
}

pub fn ensure(id: Id) !*Account {
    if (id == 0) return error.InvalidTenant;
    if (!ready) return error.BudgetUnbound;
    if (find(id)) |acct| return acct;

    in_budget = true;
    defer in_budget = false;
    const acct = try gpa.create(Account);
    acct.* = .{};

    sl.lock();
    defer sl.unlock();
    if (accounts.get(id)) |existing| {
        gpa.destroy(acct);
        return existing;
    }
    accounts.put(id, acct) catch |err| {
        gpa.destroy(acct);
        return err;
    };
    return acct;
}

pub fn find(id: Id) ?*Account {
    if (id == 0) return null;
    sl.lock();
    defer sl.unlock();
    if (!ready) return null;
    return accounts.get(id);
}

pub fn noteCpu(id: Id, ns: u64) void {
    if (id == 0 or ns == 0) return;
    const acct = ensure(id) catch return;
    acct.addCpu(ns);
}

pub fn noteIops(id: Id, pages: u64) void {
    if (id == 0 or pages == 0) return;
    const acct = ensure(id) catch return;
    acct.addIops(pages);
}

pub fn noteMem(id: Id, delta: i64) void {
    if (id == 0 or delta == 0) return;
    const acct = find(id) orelse return;
    acct.addMem(delta);
}

/// Allocator hook. Never creates an account: the create itself
/// allocates, and doing it here would run this function again.
fn noteLive(delta: i64) void {
    if (in_budget or delta == 0) return;
    const fiber = _rt.currentFiberOrNull() orelse return;
    const id = fiber.ctx.tenant orelse return;
    noteMem(id, delta);
}

pub fn write(w: *std.Io.Writer) !void {
    if (!ready) return;
    in_budget = true;
    defer in_budget = false;

    sl.lock();
    const n = accounts.count();
    const snaps = gpa.alloc(Snap, n) catch {
        sl.unlock();
        return error.OutOfMemory;
    };
    var i: usize = 0;
    var it = accounts.iterator();
    while (it.next()) |entry| {
        const acct = entry.value_ptr.*;
        snaps[i] = .{
            .id = entry.key_ptr.*,
            .cpu_ns = acct.cpu_ns.load(.monotonic),
            .mem_live = acct.mem_live.load(.monotonic),
            .iops = acct.iops.load(.monotonic),
            .cpu_cap = acct.cpu_ns_per_s.load(.monotonic),
            .mem_cap = acct.mem_cap.load(.monotonic),
            .iops_cap = acct.iops_per_s.load(.monotonic),
        };
        i += 1;
    }
    sl.unlock();
    defer gpa.free(snaps);

    try w.writeAll("# TYPE egres_tenant_cpu_ns_total counter\n");
    try w.writeAll("# TYPE egres_tenant_memory_bytes gauge\n");
    try w.writeAll("# TYPE egres_tenant_iops_total counter\n");
    try w.writeAll("# TYPE egres_tenant_cpu_cap_ns gauge\n");
    try w.writeAll("# TYPE egres_tenant_memory_cap_bytes gauge\n");
    try w.writeAll("# TYPE egres_tenant_iops_cap gauge\n");
    for (snaps[0..i]) |snap| {
        try w.print(
            "egres_tenant_cpu_ns_total{{tenant=\"{d}\"}} {d}\n",
            .{ snap.id, snap.cpu_ns },
        );
        try w.print(
            "egres_tenant_memory_bytes{{tenant=\"{d}\"}} {d}\n",
            .{ snap.id, snap.mem_live },
        );
        try w.print(
            "egres_tenant_iops_total{{tenant=\"{d}\"}} {d}\n",
            .{ snap.id, snap.iops },
        );
        try w.print(
            "egres_tenant_cpu_cap_ns{{tenant=\"{d}\"}} {d}\n",
            .{ snap.id, snap.cpu_cap },
        );
        try w.print(
            "egres_tenant_memory_cap_bytes{{tenant=\"{d}\"}} {d}\n",
            .{ snap.id, snap.mem_cap },
        );
        try w.print(
            "egres_tenant_iops_cap{{tenant=\"{d}\"}} {d}\n",
            .{ snap.id, snap.iops_cap },
        );
    }
}
