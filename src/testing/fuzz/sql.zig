//! Fuzz harness for the SQL parser.

const std = @import("std");
const fuzz = @import("mod.zig");
const common = @import("common.zig");
const metricity = @import("../../root.zig");

const sql_parser = metricity.sql.parser;

pub const Context = struct {
    allocator: std.mem.Allocator,
    fuzz_config: fuzz.Config = .{
        .max_iterations = 256,
        .max_input_len = 2048,
    },
    structured_bias: u8 = 7,

    pub fn init(allocator: std.mem.Allocator) Context {
        return .{ .allocator = allocator };
    }

    pub fn generateInput(
        self: *Context,
        prng: *std.Random.DefaultPrng,
        allocator: std.mem.Allocator,
    ) ![]u8 {
        const random = prng.random();
        if (self.structured_bias != 0 and random.intRangeLessThan(u8, 0, 10) < self.structured_bias) {
            return try generateSelect(prng, allocator);
        }
        return try common.randomBytes(prng, allocator, self.fuzz_config.max_input_len);
    }
};

pub fn testOne(ctx: *Context, input: []const u8) !void {
    var arena_inst = std.heap.ArenaAllocator.init(ctx.allocator);
    defer arena_inst.deinit();

    const arena = arena_inst.allocator();
    const stmt = sql_parser.parseSelect(arena, input) catch {
        return;
    };

    const gpa = ctx.allocator;

    const analysis = metricity.sql.semantics.analyzeSelect(gpa, stmt) catch {
        return;
    };
    defer analysis.deinit(gpa);

    var program = metricity.sql.runtime.compile(gpa, stmt, .{}) catch {
        return;
    };
    defer program.deinit();

    const generated = metricity.sql.codegen.generateExpressions(gpa, stmt) catch {
        return;
    };
    defer generated.deinit(gpa);
}

const corpus = [_][]const u8{
    "SELECT 1",
    "SELECT message FROM logs WHERE level = 'info'",
};

test "sql parser fuzz" {
    var ctx = Context.init(std.testing.allocator);
    try std.testing.fuzz(&ctx, testOne, .{ .corpus = &corpus });
}

fn generateSelect(prng: *std.Random.DefaultPrng, allocator: std.mem.Allocator) ![]u8 {
    var builder = std.ArrayList(u8){};
    defer builder.deinit(allocator);
    var writer = builder.writer(allocator);

    const select_variants = [_][]const u8{
        "message",
        "value",
        "value + 1 AS next_value",
        "message, value",
        "source_id",
    };
    const idx = prng.random().intRangeLessThan(usize, 0, select_variants.len);
    try writer.writeAll("SELECT ");
    try writer.writeAll(select_variants[idx]);
    try writer.writeAll(" FROM logs");

    if (prng.random().boolean()) {
        try writer.writeAll(" WHERE ");
        if (prng.random().boolean()) {
            const comparisons = [_][]const u8{
                "level = 'info'",
                "level = 'error'",
                "message <> ''",
            };
            const cidx = prng.random().intRangeLessThan(usize, 0, comparisons.len);
            try writer.writeAll(comparisons[cidx]);
        } else {
            const op = if (prng.random().boolean()) ">" else "<";
            const threshold = 10 + prng.random().intRangeLessThan(u32, 0, 90);
            try writer.print("value {s} {d}", .{ op, threshold });
        }
    }

    if (prng.random().intRangeLessThan(u8, 0, 5) == 0) {
        try writer.writeAll(" ORDER BY message");
    }

    try writer.writeByte('\n');
    return builder.toOwnedSlice(allocator);
}
