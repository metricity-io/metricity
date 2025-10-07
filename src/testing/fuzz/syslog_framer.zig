//! Fuzz harness for the syslog framer.

const std = @import("std");
const fuzz = @import("mod.zig");
const source = @import("source");
const frame = source.syslog_frame;

pub const Context = struct {
    allocator: std.mem.Allocator,
    fuzz_config: fuzz.Config = .{
        .max_iterations = 256,
        .max_input_len = 4096,
    },

    pub fn init(allocator: std.mem.Allocator) Context {
        return .{ .allocator = allocator };
    }
};

fn takeByte(input: []const u8, index: *usize) ?u8 {
    if (index.* >= input.len) return null;
    const value = input[index.*];
    index.* += 1;
    return value;
}

fn pushRandomSlice(framer: *frame.Framer, input: []const u8, index: *usize) !void {
    const len_byte = takeByte(input, index) orelse return;
    const remaining = input.len - index.*;
    const take = @min(@as(usize, len_byte), remaining);
    try framer.push(input[index.* .. index.* + take]);
    index.* += take;
}

fn pushMalformedLength(framer: *frame.Framer, seed: u8) !void {
    var buffer: [64]u8 = undefined;
    const digits = 1 + @as(usize, seed % 12);
    var i: usize = 0;
    while (i < digits) : (i += 1) {
        buffer[i] = '0' + (seed % 10);
    }
    buffer[digits] = 'x';
    buffer[digits + 1] = '\n';
    try framer.push(buffer[0 .. digits + 2]);
}

fn pushOverflowLength(framer: *frame.Framer) !void {
    var buffer: [96]u8 = undefined;
    var i: usize = 0;
    while (i < 32) : (i += 1) {
        buffer[i] = '9';
    }
    buffer[i] = ' ';
    buffer[i + 1] = 'f';
    buffer[i + 2] = 'o';
    buffer[i + 3] = 'o';
    buffer[i + 4] = '\n';
    try framer.push(buffer[0 .. i + 5]);
}

fn pushLfFrame(
    framer: *frame.Framer,
    seed: u8,
) !void {
    var buffer: [128]u8 = undefined;
    const len = 1 + @as(usize, seed % 64);
    var i: usize = 0;
    while (i < len) : (i += 1) {
        const offset_usize = (@as(usize, seed) + i) % 26;
        const char_offset: u8 = @intCast(offset_usize);
        buffer[i] = 'a' + char_offset;
    }
    buffer[len] = '\n';
    try framer.push(buffer[0 .. len + 1]);
}

fn pushOctetFrame(
    framer: *frame.Framer,
    seed: u8,
) !void {
    var buffer: [160]u8 = undefined;
    const payload_len = 1 + @as(usize, seed % 64);
    const header = std.fmt.bufPrint(buffer[0..], "{d} ", .{payload_len}) catch return;
    const header_len = header.len;
    var i: usize = 0;
    while (i < payload_len) : (i += 1) {
        const offset_usize = (@as(usize, seed) + (i * 3)) % 26;
        const char_offset: u8 = @intCast(offset_usize);
        buffer[header_len + i] = 'A' + char_offset;
    }
    try framer.push(buffer[0 .. header_len + payload_len]);
}

fn stepNext(ctx: *Context, framer: *frame.Framer) !void {
    const before = framer.dropStats();
    const result = framer.next() catch |err| {
        const after = framer.dropStats();
        switch (err) {
            frame.FrameError.InvalidLength => {
                try std.testing.expectEqual(before.length + 1, after.length);
                try std.testing.expectEqual(before.overflow, after.overflow);
                try std.testing.expectEqual(before.invalid, after.invalid);
                return;
            },
            frame.FrameError.Overflow => {
                try std.testing.expectEqual(before.overflow + 1, after.overflow);
                try std.testing.expectEqual(before.length, after.length);
                try std.testing.expectEqual(before.invalid, after.invalid);
                return;
            },
            else => {
                // OutOfMemory or other unexpected error ends the iteration.
                return;
            },
        }
    };

    const maybe_frame = result orelse return;
    defer ctx.allocator.free(maybe_frame.payload);

    try std.testing.expect(maybe_frame.payload.len <= framer.message_limit);
}

fn drainBuffer(ctx: *Context, framer: *frame.Framer, assume_truncated: bool) !void {
    const drained = framer.drainBuffered(assume_truncated) catch {
        return;
    };
    const maybe_frame = drained orelse return;
    defer ctx.allocator.free(maybe_frame.payload);
    try std.testing.expect(maybe_frame.payload.len <= framer.message_limit);
}

pub fn testOne(ctx: *Context, input: []const u8) !void {
    if (input.len == 0) return;

    var index: usize = 0;
    const seed_limit = takeByte(input, &index) orelse return;
    const seed_mode = takeByte(input, &index) orelse 0;

    const message_limit = 1 + @as(usize, seed_limit % 128);
    const selected_mode = switch (seed_mode % 3) {
        0 => frame.Mode.auto,
        1 => frame.Mode.lf,
        else => frame.Mode.octet,
    };

    var framer = frame.Framer.init(ctx.allocator, selected_mode, message_limit);
    defer framer.deinit();

    while (index < input.len) {
        const opcode = takeByte(input, &index) orelse break;
        switch (opcode % 8) {
            0 => try pushRandomSlice(&framer, input, &index),
            1 => try pushMalformedLength(&framer, opcode),
            2 => try pushOverflowLength(&framer),
            3 => try pushLfFrame(&framer, opcode),
            4 => try pushOctetFrame(&framer, opcode),
            5 => framer.reset(),
            6 => try drainBuffer(ctx, &framer, (opcode & 0x1) == 1),
            else => {}, // fallthrough to processing
        }

        var steps: usize = 0;
        while (steps < 4 and framer.hasBufferedData()) : (steps += 1) {
            try stepNext(ctx, &framer);
        }
    }

    if (framer.hasBufferedData()) {
        try drainBuffer(ctx, &framer, true);
    }
}

const corpus = [_][]const u8{
    "auto\n",
    "octet 5 hello\n",
    "18446744073709551616 foo\n",
};

test "syslog framer fuzz" {
    var ctx = Context.init(std.testing.allocator);
    try std.testing.fuzz(&ctx, testOne, .{ .corpus = &corpus });
}
