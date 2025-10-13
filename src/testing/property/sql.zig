//! Property-based tests for SQL runtime.

const std = @import("std");
const property = @import("mod.zig");
const metricity = @import("../../root.zig");

const event_mod = metricity.source.event;

fn makeEvent(allocator: std.mem.Allocator, a: i64, b: i64, c: i64) !event_mod.Event {
    var fields = try allocator.alloc(event_mod.Field, 3);
    fields[0] = .{ .name = "a", .value = .{ .integer = a } };
    fields[1] = .{ .name = "b", .value = .{ .integer = b } };
    fields[2] = .{ .name = "c", .value = .{ .integer = c } };
    return event_mod.Event{
        .metadata = .{ .source_id = "assoc" },
        .payload = .{ .log = .{ .message = "assoc", .fields = fields } },
    };
}

fn freeEvent(allocator: std.mem.Allocator, ev: event_mod.Event) void {
    switch (ev.payload) {
        .log => |log_event| allocator.free(log_event.fields),
    }
}

fn compileProgram(allocator: std.mem.Allocator, query: []const u8) !metricity.sql.runtime.Program {
    var arena_inst = std.heap.ArenaAllocator.init(allocator);
    defer arena_inst.deinit();

    const stmt = try metricity.sql.parser.parseSelect(arena_inst.allocator(), query);
    return try metricity.sql.runtime.compile(allocator, stmt, .{});
}

const AssocSuite = struct {
    program_left: metricity.sql.runtime.Program,
    program_right: metricity.sql.runtime.Program,

    fn init(allocator: std.mem.Allocator) !AssocSuite {
        return AssocSuite{
            .program_left = try compileProgram(allocator, "SELECT SUM(a + (b + c)) AS value FROM logs"),
            .program_right = try compileProgram(allocator, "SELECT SUM((a + b) + c) AS value FROM logs"),
        };
    }

    fn deinit(self: *AssocSuite) void {
        self.program_left.deinit();
        self.program_right.deinit();
    }
};

fn executeProgram(program: *metricity.sql.runtime.Program, event: *const event_mod.Event) !?metricity.sql.runtime.Row {
    const now_ns: u64 = @intCast(std.time.nanoTimestamp());
    return try program.execute(std.testing.allocator, event, now_ns);
}

fn compareRows(left: ?metricity.sql.runtime.Row, right: ?metricity.sql.runtime.Row) !void {
    if ((left == null) or (right == null)) {
        try std.testing.expect(left == null);
        try std.testing.expect(right == null);
        if (left) |row| row.deinit();
        if (right) |row| row.deinit();
        return;
    }
    var l = left.?;
    defer l.deinit();
    var r = right.?;
    defer r.deinit();
    try std.testing.expectEqual(@as(usize, 1), l.values.len);
    try std.testing.expectEqual(@as(usize, 1), r.values.len);
    try std.testing.expect(l.values[0].value == r.values[0].value);
}

test "integer addition associativity" {
    var suite = try AssocSuite.init(std.testing.allocator);
    defer suite.deinit();

    var prng = property.initPrng(0xBEEF);

    var i: usize = 0;
    while (i < 200) : (i += 1) {
        const a = property.nextRange(&prng, i64, -1_000, 1_000);
        const b = property.nextRange(&prng, i64, -1_000, 1_000);
        const c = property.nextRange(&prng, i64, -1_000, 1_000);

        var event = try makeEvent(std.testing.allocator, a, b, c);
        defer freeEvent(std.testing.allocator, event);

        const left_row = try executeProgram(&suite.program_left, &event);
        const right_row = try executeProgram(&suite.program_right, &event);
        try compareRows(left_row, right_row);
        suite.program_left.reset();
        suite.program_right.reset();
    }
}
