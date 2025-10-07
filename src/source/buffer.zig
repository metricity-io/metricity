const std = @import("std");

/// Policy that determines how the buffer behaves when it reaches capacity.
pub const WhenFull = enum {
    /// The producer receives `error.WouldBlock` and must decide what to do next.
    reject,
    /// The newest element is dropped while keeping previously queued items.
    drop_newest,
    /// The oldest element is removed to make room for the new one.
    drop_oldest,
};

pub const PushError = error{
    WouldBlock,
};

/// Basic bounded ring buffer intended for connecting sources to downstream
/// consumers. The buffer favors predictable memory usage and exposes surface
/// area for backpressure.
pub fn BoundedBuffer(comptime T: type) type {
    return struct {
        const Self = @This();

        pub const PushOutcome = union(enum) {
            stored,
            dropped_newest,
            dropped_oldest: T,
        };

        allocator: std.mem.Allocator,
        storage: []T,
        head: usize = 0,
        count: usize = 0,
        when_full: WhenFull,
        drops_total: usize = 0,

        pub fn init(
            allocator: std.mem.Allocator,
            requested_capacity: usize,
            when_full: WhenFull,
        ) !Self {
            std.debug.assert(requested_capacity > 0);
            const storage = try allocator.alloc(T, requested_capacity);
            return Self{
                .allocator = allocator,
                .storage = storage,
                .when_full = when_full,
            };
        }

        pub fn drainWith(self: *Self, handler: anytype) void {
            while (self.pop()) |item| {
                handler(item);
            }
        }

        pub fn deinit(self: *Self) void {
            std.debug.assert(self.count == 0);
            self.allocator.free(self.storage);
            self.* = undefined;
        }

        pub fn deinitWith(self: *Self, handler: anytype) void {
            self.drainWith(handler);
            self.deinit();
        }

        pub fn capacity(self: *const Self) usize {
            return self.storage.len;
        }

        pub fn len(self: *const Self) usize {
            return self.count;
        }

        pub fn isEmpty(self: *const Self) bool {
            return self.count == 0;
        }

        pub fn isFull(self: *const Self) bool {
            return self.count == self.storage.len;
        }

        pub fn drops(self: *const Self) usize {
            return self.drops_total;
        }

        pub fn push(self: *Self, item: T) PushError!PushOutcome {
            var evicted: ?T = null;
            if (self.isFull()) {
                switch (self.when_full) {
                    .reject => return PushError.WouldBlock,
                    .drop_newest => {
                        self.drops_total += 1;
                        return PushOutcome{ .dropped_newest = {} };
                    },
                    .drop_oldest => {
                        evicted = self.storage[self.head];
                        self.head = (self.head + 1) % self.storage.len;
                        self.count -= 1;
                        self.drops_total += 1;
                    },
                }
            }

            const slot = (self.head + self.count) % self.storage.len;
            self.storage[slot] = item;
            self.count += 1;
            return if (evicted) |value|
                PushOutcome{ .dropped_oldest = value }
            else
                PushOutcome{ .stored = {} };
        }

        pub fn pop(self: *Self) ?T {
            if (self.isEmpty()) return null;
            const item = self.storage[self.head];
            self.head = (self.head + 1) % self.storage.len;
            self.count -= 1;
            return item;
        }
    };
}

test "drop_newest policy increments drop count" {
    const allocator = std.testing.allocator;
    const Buffer = BoundedBuffer(u32);
    const Outcome = Buffer.PushOutcome;
    var buffer = try Buffer.init(allocator, 2, .drop_newest);
    defer buffer.deinit();

    try std.testing.expectEqual(Outcome{ .stored = {} }, try buffer.push(1));
    try std.testing.expectEqual(Outcome{ .stored = {} }, try buffer.push(2));
    try std.testing.expectEqual(Outcome{ .dropped_newest = {} }, try buffer.push(3));
    try std.testing.expectEqual(@as(usize, 1), buffer.drops());
    try std.testing.expectEqual(@as(usize, 2), buffer.len());

    try std.testing.expectEqual(@as(?u32, 1), buffer.pop());
    try std.testing.expectEqual(@as(?u32, 2), buffer.pop());
    try std.testing.expectEqual(@as(?u32, null), buffer.pop());
}

test "reject policy signals producer" {
    const allocator = std.testing.allocator;
    const Buffer = BoundedBuffer(u32);
    const Outcome = Buffer.PushOutcome;
    var buffer = try Buffer.init(allocator, 1, .reject);
    defer buffer.deinit();

    try std.testing.expectEqual(Outcome{ .stored = {} }, try buffer.push(1));
    try std.testing.expectError(PushError.WouldBlock, buffer.push(2));
    try std.testing.expectEqual(@as(?u32, 1), buffer.pop());
    try std.testing.expectEqual(Outcome{ .stored = {} }, try buffer.push(3));
    try std.testing.expectEqual(@as(?u32, 3), buffer.pop());
}

test "drop_oldest policy evicts head and stores new item" {
    const allocator = std.testing.allocator;
    const Buffer = BoundedBuffer(u32);
    const Outcome = Buffer.PushOutcome;
    var buffer = try Buffer.init(allocator, 2, .drop_oldest);
    defer buffer.deinit();

    try std.testing.expectEqual(Outcome{ .stored = {} }, try buffer.push(1));
    try std.testing.expectEqual(Outcome{ .stored = {} }, try buffer.push(2));

    const outcome = try buffer.push(3);
    try std.testing.expect(outcome == .dropped_oldest);
    try std.testing.expectEqual(@as(u32, 1), outcome.dropped_oldest);
    try std.testing.expectEqual(@as(usize, 1), buffer.drops());

    try std.testing.expectEqual(@as(?u32, 2), buffer.pop());
    try std.testing.expectEqual(@as(?u32, 3), buffer.pop());
    try std.testing.expectEqual(@as(?u32, null), buffer.pop());
}

test "drainWith executes handler for remaining items" {
    const allocator = std.testing.allocator;
    const Buffer = BoundedBuffer(u32);
    var buffer = try Buffer.init(allocator, 4, .reject);
    defer buffer.deinit();

    _ = try buffer.push(5);
    _ = try buffer.push(7);

    const Tracker = struct {
        var sum: usize = 0;

        fn record(value: u32) void {
            sum += value;
        }
    };

    buffer.drainWith(Tracker.record);
    try std.testing.expectEqual(@as(usize, 12), Tracker.sum);
    try std.testing.expect(buffer.isEmpty());
}
