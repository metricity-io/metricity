const std = @import("std");

pub const RingBuffer = struct {
    allocator: std.mem.Allocator,
    buffer: []u8,
    head: usize,
    tail: usize,
    count: usize,
    max_capacity: ?usize = null,

    pub fn init(allocator: std.mem.Allocator, initial_capacity: usize) !RingBuffer {
        std.debug.assert(initial_capacity > 0);
        return try initWithLimit(allocator, initial_capacity, null);
    }

    pub fn initWithLimit(
        allocator: std.mem.Allocator,
        initial_capacity: usize,
        max_capacity: ?usize,
    ) !RingBuffer {
        std.debug.assert(initial_capacity > 0);
        if (max_capacity) |limit| {
            std.debug.assert(limit >= initial_capacity);
        }
        const storage = try allocator.alloc(u8, initial_capacity);
        return .{
            .allocator = allocator,
            .buffer = storage,
            .head = 0,
            .tail = 0,
            .count = 0,
            .max_capacity = max_capacity,
        };
    }

    pub fn deinit(self: *RingBuffer) void {
        self.allocator.free(self.buffer);
        self.* = undefined;
    }

    pub fn len(self: *const RingBuffer) usize {
        return self.count;
    }

    pub fn capacity(self: *const RingBuffer) usize {
        return self.buffer.len;
    }

    pub fn isEmpty(self: *const RingBuffer) bool {
        return self.len() == 0;
    }

    pub fn ensureWriteCapacity(self: *RingBuffer, additional: usize) !void {
        if (additional == 0) return;
        if (self.buffer.len - self.count >= additional) return;

        var new_capacity = self.buffer.len;
        const target = std.math.add(usize, self.count, additional) catch return error.OutOfMemory;
        if (self.max_capacity) |limit| {
            if (target > limit) return error.MaximumCapacityExceeded;
        }
        while (new_capacity < target) {
            const doubled = std.math.mul(usize, new_capacity, 2) catch return error.OutOfMemory;
            new_capacity = if (doubled < target) target else doubled;
            if (self.max_capacity) |limit| {
                if (new_capacity > limit) {
                    new_capacity = limit;
                }
            }
        }

        if (self.max_capacity) |limit| {
            if (new_capacity > limit) return error.MaximumCapacityExceeded;
        }

        const new_buffer = try self.allocator.alloc(u8, new_capacity);
        const old_buffer = self.buffer;
        defer self.allocator.free(old_buffer);

        const segments = self.slices();
        if (segments.first.len != 0) {
            @memcpy(new_buffer[0..segments.first.len], segments.first);
        }
        if (segments.second.len != 0) {
            @memcpy(new_buffer[segments.first.len .. segments.first.len + segments.second.len], segments.second);
        }

        self.buffer = new_buffer;
        self.head = 0;
        self.tail = self.count;
    }

    pub fn write(self: *RingBuffer, data: []const u8) !void {
        if (data.len == 0) return;
        try self.ensureWriteCapacity(data.len);
        const buffer_len = self.buffer.len;
        const first_len = @min(data.len, buffer_len - self.tail);
        @memcpy(self.buffer[self.tail .. self.tail + first_len], data[0..first_len]);
        const remainder = data[first_len..];
        if (remainder.len != 0) {
            @memcpy(self.buffer[0..remainder.len], remainder);
        }
        self.tail = (self.tail + data.len) % buffer_len;
        self.count += data.len;
    }

    pub fn peekFirst(self: *const RingBuffer) []const u8 {
        if (self.count == 0) return &[_]u8{};
        if (self.head < self.tail) {
            return self.buffer[self.head..self.tail];
        }
        return self.buffer[self.head..];
    }

    pub fn peekSecond(self: *const RingBuffer) []const u8 {
        if (self.count == 0) return &[_]u8{};
        if (self.head < self.tail) {
            return &[_]u8{};
        }
        return self.buffer[0..self.tail];
    }

    pub fn slices(self: *const RingBuffer) Slices {
        return Slices{ .first = self.peekFirst(), .second = self.peekSecond() };
    }

    pub fn consume(self: *RingBuffer, count: usize) void {
        std.debug.assert(count <= self.count);
        if (count == 0) return;
        if (count == self.count) {
            self.head = 0;
            self.tail = 0;
            self.count = 0;
            return;
        }
        self.head = (self.head + count) % self.buffer.len;
        self.count -= count;
    }

    pub fn copyOut(self: *const RingBuffer, dest: []u8) void {
        std.debug.assert(dest.len <= self.count);
        if (dest.len == 0) return;
        const segments = self.slices();
        var copied: usize = 0;
        if (segments.first.len != 0) {
            const take = @min(dest.len, segments.first.len);
            @memcpy(dest[0..take], segments.first[0..take]);
            copied += take;
            if (copied == dest.len) return;
        }
        if (segments.second.len != 0) {
            const remaining = dest.len - copied;
            std.debug.assert(remaining <= segments.second.len);
            @memcpy(dest[copied .. copied + remaining], segments.second[0..remaining]);
        }
    }

    pub fn dropFront(self: *RingBuffer, count: usize) void {
        self.consume(count);
    }

    pub fn get(self: *const RingBuffer, index: usize) u8 {
        std.debug.assert(index < self.count);
        const pos = (self.head + index) % self.buffer.len;
        return self.buffer[pos];
    }

    pub const Slices = struct {
        first: []const u8,
        second: []const u8,
    };
};

test "ring buffer wrap and copy" {
    const allocator = std.testing.allocator;
    var ring = try RingBuffer.init(allocator, 4);
    defer ring.deinit();

    try ring.write("ab");
    try ring.write("cd");
    try std.testing.expectEqual(@as(usize, 4), ring.len());

    ring.consume(3);
    try std.testing.expectEqual(@as(usize, 1), ring.len());

    try ring.write("ef");
    try std.testing.expectEqual(@as(usize, 3), ring.len());

    const slices = ring.slices();
    try std.testing.expect(slices.first.len > 0);

    var buffer: [3]u8 = undefined;
    ring.copyOut(buffer[0..buffer.len]);
    try std.testing.expectEqualStrings("def", buffer[0..]);

    try std.testing.expectEqual(@as(u8, 'd'), ring.get(0));
    try std.testing.expectEqual(@as(u8, 'f'), ring.get(2));
}

test "ring buffer ensureWriteCapacity grows and preserves order" {
    const allocator = std.testing.allocator;
    var ring = try RingBuffer.init(allocator, 2);
    defer ring.deinit();

    try ring.write("ab");
    try std.testing.expectEqual(@as(usize, 2), ring.capacity());

    try ring.write("cde");
    try std.testing.expect(ring.capacity() >= 5);
    try std.testing.expectEqual(@as(usize, 5), ring.len());

    var buffer: [5]u8 = undefined;
    ring.copyOut(buffer[0..]);
    try std.testing.expectEqualStrings("abcde", buffer[0..]);
}

test "ring buffer respects max capacity" {
    const allocator = std.testing.allocator;
    var ring = try RingBuffer.initWithLimit(allocator, 2, 4);
    defer ring.deinit();

    try ring.write("ab");
    try ring.write("cd");
    try std.testing.expectEqual(@as(usize, 4), ring.len());

    try std.testing.expectError(error.MaximumCapacityExceeded, ring.write("e"));
}
