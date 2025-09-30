//! By convention, root.zig is the root source file when making a library.
const std = @import("std");
pub const sql = @import("sql/mod.zig");
pub const source = @import("source/mod.zig");


pub fn bufferedPrint() !void {
    // Stdout is for the actual output of your application, for example if you
    // are implementing gzip, then only the compressed bytes should be sent to
    // stdout, not any debugging messages.
    var stdout_buffer: [1024]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
    const stdout = &stdout_writer.interface;

    try stdout.print("Run `zig build test` to run the tests.\n", .{});

    try stdout.flush(); // Don't forget to flush!
}

pub fn add(a: i32, b: i32) i32 {
    return a + b;
}

test "basic add functionality" {
    try std.testing.expect(add(3, 7) == 10);
}

test "sql end-to-end pipeline" {
    var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_inst.deinit();
    const arena_alloc = arena_inst.allocator();

    const stmt = try sql.parser.parseSelect(arena_alloc, "SELECT message FROM logs WHERE level = 'info'");

    const gpa = std.testing.allocator;
    const analysis = try sql.semantics.analyzeSelect(gpa, stmt);
    defer analysis.deinit(gpa);
    try std.testing.expectEqual(@as(usize, 2), analysis.columns.len);

    const generated = try sql.codegen.generateExpressions(gpa, stmt);
    defer generated.deinit(gpa);
    try std.testing.expectEqual(@as(usize, 1), generated.expressions.len);
}
