const std = @import("std");

pub const FrameError = error{
    Incomplete,
    InvalidLength,
    Overflow,
};

const FramerError = FrameError || error{OutOfMemory};

pub const ExtractResult = struct {
    frame: []const u8,
    consumed: usize,
};

pub const Mode = enum {
    auto,
    lf,
    octet,
};

pub const FramerResult = struct {
    payload: []u8,
    truncated: bool,
};

pub const Framer = struct {
    allocator: std.mem.Allocator,
    buffer: std.ArrayList(u8),
    mode: Mode,
    active_mode: Mode,
    message_limit: usize,

    pub fn init(allocator: std.mem.Allocator, mode: Mode, message_limit: usize) Framer {
        return .{
            .allocator = allocator,
            .buffer = .{},
            .mode = mode,
            .active_mode = .auto,
            .message_limit = message_limit,
        };
    }

    pub fn deinit(self: *Framer) void {
        self.buffer.deinit(self.allocator);
    }

    pub fn reset(self: *Framer) void {
        self.buffer.clearRetainingCapacity();
        self.active_mode = .auto;
    }

    pub fn push(self: *Framer, chunk: []const u8) !void {
        try self.buffer.appendSlice(self.allocator, chunk);
    }

    pub fn next(self: *Framer) FramerError!?FramerResult {
        while (true) {
            if (self.buffer.items.len == 0) return null;

            if (self.mode == .auto and self.active_mode == .auto) {
                const slice = self.buffer.items;
                const attempt = extractOctetCount(slice) catch |err| switch (err) {
                    FrameError.Incomplete => return null,
                    FrameError.InvalidLength, FrameError.Overflow => {
                        self.dropUntilNewline();
                        return err;
                    },
                };

                if (attempt) |result| {
                    self.active_mode = .octet;
                    return self.finishFrame(result);
                }

                if (extractLf(slice)) |result| {
                    self.active_mode = .lf;
                    return self.finishFrame(result);
                }

                return null;
            }

            const active = if (self.mode == .auto) self.active_mode else self.mode;
            switch (active) {
                .octet => {
                    const slice = self.buffer.items;
                    const attempt = extractOctetCount(slice) catch |err| switch (err) {
                        FrameError.Incomplete => return null,
                        FrameError.InvalidLength, FrameError.Overflow => {
                            self.dropUntilNewline();
                            self.active_mode = .auto;
                            return err;
                        },
                    };

                    if (attempt) |result| {
                        return self.finishFrame(result);
                    }

                    return null;
                },
                .lf => {
                    const slice = self.buffer.items;
                    if (extractLf(slice)) |result| {
                        return self.finishFrame(result);
                    }
                    return null;
                },
                .auto => continue,
            }
        }
    }

    fn finishFrame(self: *Framer, result: ExtractResult) FramerError!?FramerResult {
        const payload_len = result.frame.len;
        const take = @min(payload_len, self.message_limit);
        const payload = try self.allocator.alloc(u8, take);
        @memcpy(payload, result.frame[0..take]);
        const truncated = payload_len > self.message_limit;

        self.consume(result.consumed);
        return FramerResult{ .payload = payload, .truncated = truncated };
    }

    fn dropUntilNewline(self: *Framer) void {
        const slice = self.buffer.items;
        if (slice.len == 0) return;
        if (std.mem.indexOfScalar(u8, slice, '\n')) |idx| {
            self.consume(idx + 1);
        } else {
            self.buffer.clearRetainingCapacity();
        }
        self.active_mode = .auto;
    }

    fn consume(self: *Framer, count: usize) void {
        if (count == 0) return;
        const slice = self.buffer.items;
        std.debug.assert(count <= slice.len);
        const remaining = slice.len - count;
        if (remaining == 0) {
            self.buffer.clearRetainingCapacity();
            return;
        }
        std.mem.copyForwards(u8, slice[0..remaining], slice[count..]);
        self.buffer.items = slice[0..remaining];
    }

    pub fn hasBufferedData(self: *const Framer) bool {
        return self.buffer.items.len != 0;
    }
};

pub fn extractLf(buffer: []const u8) ?ExtractResult {
    if (buffer.len == 0) return null;
    const maybe_index = std.mem.indexOfScalar(u8, buffer, '\n') orelse return null;
    const end_index = if (maybe_index > 0 and buffer[maybe_index - 1] == '\r')
        maybe_index - 1
    else
        maybe_index;
    return ExtractResult{
        .frame = buffer[0..end_index],
        .consumed = maybe_index + 1,
    };
}

pub fn extractOctetCount(buffer: []const u8) FrameError!?ExtractResult {
    if (buffer.len == 0) return null;

    var index: usize = 0;
    var length: usize = 0;
    var saw_digit = false;

    while (index < buffer.len) : (index += 1) {
        const ch = buffer[index];
        if (ch >= '0' and ch <= '9') {
            saw_digit = true;
            const digit: usize = ch - '0';
            length = std.math.mul(usize, length, 10) catch return FrameError.Overflow;
            length = std.math.add(usize, length, digit) catch return FrameError.Overflow;
        } else {
            break;
        }
    }

    if (!saw_digit) return null;
    if (index >= buffer.len) return FrameError.Incomplete;
    if (buffer[index] != ' ') return FrameError.InvalidLength;

    index += 1;
    if (buffer.len - index < length) return FrameError.Incomplete;

    return ExtractResult{
        .frame = buffer[index .. index + length],
        .consumed = index + length,
    };
}

test "framer extracts lf frames" {
    const allocator = std.testing.allocator;
    var framer = Framer.init(allocator, .auto, 128);
    defer framer.deinit();

    try framer.push("first\nsecond\n");

    const first = (try framer.next()).?;
    defer allocator.free(first.payload);
    try std.testing.expectEqualStrings("first", first.payload);
    try std.testing.expect(!first.truncated);

    const second = (try framer.next()).?;
    defer allocator.free(second.payload);
    try std.testing.expectEqualStrings("second", second.payload);
    try std.testing.expect(!second.truncated);

    try std.testing.expect((try framer.next()) == null);
}

test "framer truncates messages over limit" {
    const allocator = std.testing.allocator;
    var framer = Framer.init(allocator, .auto, 4);
    defer framer.deinit();

    try framer.push("5 hello");

    const result = (try framer.next()).?;
    defer allocator.free(result.payload);
    try std.testing.expectEqualStrings("hell", result.payload);
    try std.testing.expect(result.truncated);
    try std.testing.expect((try framer.next()) == null);
}
