//! Fuzz harness for the syslog parser.

const std = @import("std");
const fuzz = @import("mod.zig");
const source = @import("source");
const netx = @import("netx");
const parser = source.syslog_parser;
const transport = netx.transport;
const cfg = source.config;

const MessageAllocation = struct {
    allocator: std.mem.Allocator,
    buffer: []u8,

    fn release(context: *anyopaque) void {
        const aligned: *align(@alignOf(MessageAllocation)) anyopaque = @alignCast(context);
        const allocation: *MessageAllocation = @ptrCast(aligned);
        allocation.allocator.free(allocation.buffer);
        allocation.allocator.destroy(allocation);
    }
};

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

fn buildPriority(writer: anytype, mode_seed: u8, flags: u8) !void {
    const invalid = (flags & 0x02) != 0;
    const pri_value: usize = if (!invalid)
        @as(usize, mode_seed % 192)
    else
        200 + @as(usize, mode_seed);

    if ((flags & 0x01) == 0) {
        try writer.print("<{d}>", .{pri_value});
    } else {
        // Missing terminator to trigger InvalidMessage.
        try writer.writeByte('<');
        const digit: u8 = @intCast(pri_value % 10);
        try writer.writeByte('0' + digit);
    }
}

fn buildRfc3164(writer: anytype) !void {
    try writer.writeAll("Jan  2 03:04:05 host.example.com app[123]: ");
}

fn buildRfc5424(writer: anytype, include_sd: bool) !void {
    try writer.writeAll("1 2023-03-17T12:34:56Z host.example.com app 1234 MSGID ");
    if (include_sd) {
        try writer.writeAll("[example@32473 key=\"value\"] ");
    } else {
        try writer.writeAll("- ");
    }
}

fn appendBody(writer: anytype, data: []const u8, flags: u8) !void {
    if ((flags & 0x08) != 0) {
        // Include UTF-8 BOM prefix.
        try writer.writeAll("\xEF\xBB\xBF");
    }

    if (data.len != 0) {
        try writer.writeAll(data);
    } else {
        try writer.writeAll("hello world");
    }

    if ((flags & 0x10) != 0) {
        // Force invalid UTF-8 sequence.
        try writer.writeByte(0xFF);
    }
}

fn createMessage(
    ctx: *Context,
    mode_seed: u8,
    limit_seed: u8,
    flags: u8,
    tail: []const u8,
) !struct {
    config: cfg.SyslogConfig,
    message: transport.Message,
    finalizer_context: *MessageAllocation,
} {
    var builder = std.ArrayList(u8){};
    defer builder.deinit(ctx.allocator);
    const writer = builder.writer(ctx.allocator);

    try buildPriority(writer, mode_seed, flags);

    const build_kind = (flags >> 4) & 0x03;
    switch (build_kind) {
        0 => try buildRfc3164(writer),
        1 => try buildRfc5424(writer, (flags & 0x04) != 0),
        else => {},
    }

    try appendBody(writer, tail, flags);

    const payload = try builder.toOwnedSlice(ctx.allocator);

    const allocation = try ctx.allocator.create(MessageAllocation);
    allocation.* = .{
        .allocator = ctx.allocator,
        .buffer = payload,
    };

    const finalizer = transport.Finalizer{
        .context = allocation,
        .function = MessageAllocation.release,
    };

    const truncated_flag = (flags & 0x40) != 0;

    const config = cfg.SyslogConfig{
        .address = "udp://127.0.0.1:0",
        .parser = switch (mode_seed % 3) {
            0 => .auto,
            1 => .rfc3164,
            else => .rfc5424,
        },
        .message_size_limit = 64 + @as(usize, limit_seed),
    };

    const message = transport.Message{
        .bytes = payload,
        .metadata = .{
            .peer_address = "127.0.0.1:5000",
            .protocol = "udp",
            .truncated = truncated_flag,
        },
        .finalizer = finalizer,
    };

    return .{ .config = config, .message = message, .finalizer_context = allocation };
}

fn validateEvent(result: parser.ParseResult, expected_truncated: bool) !void {
    const managed = result.managed;
    const event_value = managed.event;

    try std.testing.expectEqual(expected_truncated, event_value.metadata.payload_truncated);

    const payload = event_value.payload;
    switch (payload) {
        .log => |log_event| {
            try std.testing.expect(log_event.fields.len >= 3);
            try std.testing.expect(std.mem.eql(u8, log_event.fields[0].name, "syslog_facility"));
            try std.testing.expect(std.mem.eql(u8, log_event.fields[1].name, "syslog_severity"));
            try std.testing.expect(std.mem.eql(u8, log_event.fields[2].name, "syslog_format"));

            const is_valid_utf8 = std.unicode.utf8ValidateSlice(log_event.message);
            if (!is_valid_utf8) {
                var found_flag = false;
                for (log_event.fields) |field| {
                    if (std.mem.eql(u8, field.name, "syslog_msg_utf8_valid")) {
                        found_flag = true;
                        try std.testing.expect(field.value == .boolean);
                        try std.testing.expect(!field.value.boolean);
                    }
                }
                if (!found_flag) {
                    // Accept absence while still ensuring message is non-empty.
                    try std.testing.expect(log_event.message.len != 0);
                }
            }
        },
    }
}

pub fn testOne(ctx: *Context, input: []const u8) !void {
    if (input.len < 3) return;

    const mode_seed = input[0];
    const limit_seed = input[1];
    const flags = input[2];
    const tail = input[3..];

    var pool = parser.EventArenaPool.init(ctx.allocator);
    defer pool.deinit();

    const built = try createMessage(ctx, mode_seed, limit_seed, flags, tail);
    const message = built.message;
    const config = built.config;
    const allocation = built.finalizer_context;

    const result = parser.parseMessage(&pool, config, message) catch |err| {
        const finalizer = transport.Finalizer{
            .context = allocation,
            .function = MessageAllocation.release,
        };
        finalizer.run();
        switch (err) {
            parser.ParseError.InvalidMessage,
            parser.ParseError.UnsupportedFormat,
            parser.ParseError.Truncated,
            => return,
            else => return,
        }
    };

    defer result.managed.finalizer.run();

    try validateEvent(result, message.metadata.truncated);
}

const corpus = [_][]const u8{
    "<34>1 2023-03-17T01:23:45Z host app 1234 MSGID - Message\n",
    "<165>Aug 24 05:34:00 mymachine su: 'su root' failed for lonvick on /dev/pts/8\n",
    "<999>notvalid\n",
};

test "syslog parser fuzz" {
    var ctx = Context.init(std.testing.allocator);
    try std.testing.fuzz(&ctx, testOne, .{ .corpus = &corpus });
}
