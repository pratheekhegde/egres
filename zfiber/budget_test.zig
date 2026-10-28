// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const fiber = @import("zfiber");
const clock = @import("utils").coarse_clock;

fn nowMs() i64 {
    return clock.timestampNow(.realtime, .millis);
}

const tenant_a: fiber.budget.Id = 0xB0D6_0001;
const tenant_b: fiber.budget.Id = 0xB0D6_0002;
const tenant_c: fiber.budget.Id = 0xB0D6_0003;

fn bind(id: fiber.budget.Id) void {
    fiber.ctx().tenant = id;
}

test "budget: a zero cap does not park, and metrics name the tenant" {
    bind(tenant_a);
    defer fiber.ctx().tenant = null;
    try fiber.budget.set(tenant_a, .{});
    fiber.budget.noteCpu(tenant_a, 1 << 40);
    fiber.budget.noteIops(tenant_a, 1 << 20);

    const acct = fiber.budget.find(tenant_a).?;
    try std.testing.expectEqual(0, acct.paceWaitMs());

    const t0 = nowMs();
    try fiber.ctx().maybeYield();
    try std.testing.expect(nowMs() - t0 < 200);

    var body: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer body.deinit();
    try fiber.budget.write(&body.writer);
    var label: [80]u8 = undefined;
    const needle = std.fmt.bufPrint(
        &label,
        "egres_tenant_cpu_ns_total{{tenant=\"{d}\"}}",
        .{tenant_a},
    ) catch unreachable;
    const out = body.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, out, needle) != null);
}

test "budget: memory over the cap fails the yield" {
    bind(tenant_b);
    defer fiber.ctx().tenant = null;
    try fiber.budget.set(tenant_b, .{ .mem_bytes = 64 });
    fiber.budget.noteMem(tenant_b, 128);
    try std.testing.expectError(
        error.TenantMemory,
        fiber.ctx().maybeYield(),
    );
    fiber.budget.noteMem(tenant_b, -128);
    try fiber.ctx().maybeYield();
}

test "budget: a spent cpu window parks until it turns" {
    bind(tenant_c);
    defer fiber.ctx().tenant = null;
    try fiber.budget.set(tenant_c, .{ .cpu_ns_per_s = 1 });
    fiber.budget.noteCpu(tenant_c, 1_000_000);

    const acct = fiber.budget.find(tenant_c).?;
    try std.testing.expect(acct.paceWaitMs() > 0);

    const t0 = nowMs();
    try fiber.ctx().maybeYield();
    try std.testing.expect(nowMs() - t0 >= 800);
}

test "budget: disk pages over the cap ask for a wait" {
    const id: fiber.budget.Id = 0xB0D6_0004;
    try fiber.budget.set(id, .{ .iops_per_s = 1 });
    fiber.budget.noteIops(id, 8);
    const acct = fiber.budget.find(id).?;
    try std.testing.expect(acct.paceWaitMs() > 0);
    try fiber.budget.set(id, .{ .iops_per_s = 0 });
    try std.testing.expectEqual(0, acct.paceWaitMs());
}
