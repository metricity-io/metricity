//! Fuzz harness for the configuration parser.

const std = @import("std");
const fuzz = @import("mod.zig");
const common = @import("common.zig");
const metricity = @import("../../root.zig");

const config_parser = metricity.config_parser;

pub const Context = struct {
    allocator: std.mem.Allocator,
    fuzz_config: fuzz.Config = .{
        .max_iterations = 256,
        .max_input_len = 4096,
    },
    structured_bias: u8 = 6,

    pub fn init(allocator: std.mem.Allocator) Context {
        return .{ .allocator = allocator };
    }

    pub fn generateInput(
        self: *Context,
        prng: *std.Random.DefaultPrng,
        allocator: std.mem.Allocator,
    ) ![]u8 {
        if (self.structured_bias == 0) {
            return try common.randomBytes(prng, allocator, self.fuzz_config.max_input_len);
        }

        const threshold = prng.random().intRangeLessThan(u8, 0, 10);
        if (threshold < self.structured_bias) {
            return try generateStructured(prng, allocator);
        }
        return try common.randomBytes(prng, allocator, self.fuzz_config.max_input_len);
    }
};

pub fn testOne(ctx: *Context, input: []const u8) !void {
    var arena_inst = std.heap.ArenaAllocator.init(ctx.allocator);
    defer arena_inst.deinit();

    const arena = arena_inst.allocator();

    const result = config_parser.parse(arena, input) catch {
        return;
    };
    defer result.deinit(arena);

    const gpa = ctx.allocator;
    _ = result.pipeline.validate(gpa) catch {};
}

const corpus = [_][]const u8{
    "[sources]\n[sources.syslog]\naddress = \"udp://127.0.0.1:0\"\noutputs = [\"sql\"]\n\n[transforms]\n[transforms.sql]\ninputs = [\"syslog\"]\noutputs = [\"sink\"]\nquery = \"SELECT message FROM logs\"\n\n[sinks]\n[sinks.console]\ninputs = [\"sql\"]\n\n",
};

test "config parser fuzz" {
    var ctx = Context.init(std.testing.allocator);
    try std.testing.fuzz(&ctx, testOne, .{ .corpus = &corpus });
}

fn generateStructured(prng: *std.Random.DefaultPrng, allocator: std.mem.Allocator) ![]u8 {
    var builder = std.ArrayList(u8){};
    defer builder.deinit(allocator);
    var writer = builder.writer(allocator);

    try writer.writeAll("[sources]\n");

    var source_buf: [24]u8 = undefined;
    const source_name = common.randomName(prng, "src", &source_buf, 2, 6);

    try writer.print("[sources.{s}]\n", .{source_name});
    try writer.writeAll("type = \"syslog\"\n");
    const port = 4000 + prng.random().intRangeLessThan(u16, 0, 2000);
    try writer.print("address = \"udp://127.0.0.1:{d}\"\n", .{port});

    var sink_buf: [24]u8 = undefined;
    const sink_name = common.randomName(prng, "sink", &sink_buf, 2, 6);

    var transform_buf: [24]u8 = undefined;
    const include_transform = prng.random().boolean();
    const transform_name = if (include_transform)
        common.randomName(prng, "sql", &transform_buf, 2, 6)
    else
        sink_name;

    try writer.print("outputs = [\"{s}\"]\n\n", .{transform_name});

    if (include_transform) {
        try writer.writeAll("[transforms]\n");
        try writer.print("[transforms.{s}]\n", .{transform_name});
        try writer.writeAll("type = \"sql\"\n");
        try writer.print("inputs = [\"{s}\"]\n", .{source_name});
        try writer.print("outputs = [\"{s}\"]\n", .{sink_name});
        try writer.writeAll("query = \"SELECT message, value + 1 AS next_value FROM logs\"\n");
        if (prng.random().boolean()) {
            const parallelism = prng.random().intRangeLessThan(u8, 1, 4);
            try writer.print("parallelism = {d}\n", .{parallelism});
        }
        if (prng.random().boolean()) {
            const capacity = 8 + prng.random().intRangeLessThan(u16, 0, 248);
            try writer.print("queue_capacity = {d}\n", .{capacity});
        }
        try writer.writeAll("\n");
    }

    try writer.writeAll("[sinks]\n");
    try writer.print("[sinks.{s}]\n", .{sink_name});
    try writer.writeAll("type = \"console\"\n");
    try writer.print("inputs = [\"{s}\"]\n", .{transform_name});
    if (prng.random().boolean()) {
        const parallelism = prng.random().intRangeLessThan(u8, 1, 4);
        try writer.print("parallelism = {d}\n", .{parallelism});
    }
    if (prng.random().boolean()) {
        const capacity = 8 + prng.random().intRangeLessThan(u16, 0, 248);
        try writer.print("queue_capacity = {d}\n", .{capacity});
    }

    try writer.writeByte('\n');

    return builder.toOwnedSlice(allocator);
}
