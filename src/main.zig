const std = @import("std");
const metricity = @import("metricity");

pub fn main() !void {
    // Prints to stderr, ignoring potential errors.
    std.debug.print("All your {s} are belong to us.\n", .{"codebase"});
    try metricity.bufferedPrint();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const query = "SELECT value + 1 AS next_value, message FROM logs WHERE level = 'info'";
    const stmt = try metricity.sql.parser.parseSelect(arena.allocator(), query);
    const generated = try metricity.sql.codegen.generateExpressions(allocator, stmt);
    defer generated.deinit(allocator);

    for (generated.expressions) |expr| {
        const alias = if (expr.alias) |a| a else "<expr>";
        std.debug.print("{s}: {s}\n", .{ alias, expr.code });
    }
}

test "simple test" {
    const gpa = std.testing.allocator;
    var list: std.ArrayList(i32) = .empty;
    defer list.deinit(gpa); // Try commenting this out and see if zig detects the memory leak!
    try list.append(gpa, 42);
    try std.testing.expectEqual(@as(i32, 42), list.pop());
}

test "fuzz example" {
    const Context = struct {
        fn testOne(context: @This(), input: []const u8) anyerror!void {
            _ = context;
            // Try passing `--fuzz` to `zig build test` and see if it manages to fail this test case!
            try std.testing.expect(!std.mem.eql(u8, "canyoufindme", input));
        }
    };
    try std.testing.fuzz(Context{}, Context.testOne, .{});
}
