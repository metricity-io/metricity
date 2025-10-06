const std = @import("std");
const source_mod = @import("source");
const buffer = source_mod.buffer;
const config_mod = @import("../config/mod.zig");

pub const PushError = error{ WouldBlock, Closed };

pub fn Channel(comptime T: type) type {
    return struct {
        const Buffer = buffer.BoundedBuffer(T);

        mutex: std.Thread.Mutex = .{},
        not_empty: std.Thread.Condition = .{},
        buffer: Buffer,
        closed: bool = false,

        pub fn init(allocator: std.mem.Allocator, queue: config_mod.QueueConfig) !@This() {
            const when_full = toWhenFull(queue.strategy);
            return @This(){
                .buffer = try Buffer.init(allocator, queue.capacity, when_full),
            };
        }

        pub fn deinit(self: *@This()) void {
            self.buffer.deinit();
            self.* = undefined;
        }

        pub fn push(self: *@This(), item: T) PushError!PushResult(T) {
            self.mutex.lock();
            defer self.mutex.unlock();

            if (self.closed) return PushError.Closed;

            const outcome = self.buffer.push(item) catch |err| switch (err) {
                buffer.PushError.WouldBlock => return PushError.WouldBlock,
            };

            switch (outcome) {
                .stored => {
                    self.not_empty.signal();
                    return PushResult(T){ .stored = {} };
                },
                .dropped_newest => {
                    return PushResult(T){ .dropped_newest = {} };
                },
                .dropped_oldest => |evicted| {
                    self.not_empty.signal();
                    return PushResult(T){ .dropped_oldest = evicted };
                },
            }
        }

        pub fn pop(self: *@This()) ?T {
            self.mutex.lock();
            defer self.mutex.unlock();

            while (self.buffer.len() == 0) {
                if (self.closed) return null;
                self.not_empty.wait(&self.mutex);
            }

            return self.buffer.pop();
        }

        pub fn popBatch(self: *@This(), output: []T) usize {
            std.debug.assert(output.len != 0);

            self.mutex.lock();
            defer self.mutex.unlock();

            while (self.buffer.len() == 0) {
                if (self.closed) return 0;
                self.not_empty.wait(&self.mutex);
            }

            var count: usize = 0;
            while (count < output.len) {
                const maybe = self.buffer.pop() orelse break;
                output[count] = maybe;
                count += 1;
            }
            return count;
        }

        pub fn close(self: *@This()) void {
            self.mutex.lock();
            defer self.mutex.unlock();
            if (self.closed) return;
            self.closed = true;
            self.not_empty.broadcast();
        }
    };
}

pub fn PushResult(comptime T: type) type {
    return union(enum) {
        stored,
        dropped_newest,
        dropped_oldest: T,
    };
}

fn toWhenFull(strategy: config_mod.QueueStrategy) buffer.WhenFull {
    return switch (strategy) {
        .reject => .reject,
        .drop_newest => .drop_newest,
        .drop_oldest => .drop_oldest,
    };
}

const testing = std.testing;

fn testQueueConfig() config_mod.QueueConfig {
    return .{ .capacity = 2, .strategy = .reject };
}

test "channel stores items" {
    var channel = try Channel(u32).init(testing.allocator, testQueueConfig());
    defer channel.deinit();

    try testing.expect(switch (try channel.push(1)) {
        .stored => true,
        else => false,
    });
    try testing.expect(switch (try channel.push(2)) {
        .stored => true,
        else => false,
    });
    try testing.expectError(PushError.WouldBlock, channel.push(3));

    try testing.expectEqual(@as(?u32, 1), channel.pop());
    try testing.expectEqual(@as(?u32, 2), channel.pop());
}

test "channel drop_oldest returns evicted" {
    var channel = try Channel(u32).init(testing.allocator, .{ .capacity = 2, .strategy = .drop_oldest });
    defer channel.deinit();

    try testing.expect(switch (try channel.push(1)) {
        .stored => true,
        else => false,
    });
    try testing.expect(switch (try channel.push(2)) {
        .stored => true,
        else => false,
    });
    const outcome = try channel.push(3);
    try testing.expect(switch (outcome) {
        .dropped_oldest => |_| true,
        else => false,
    });
    try testing.expectEqual(@as(u32, 1), outcome.dropped_oldest);

    try testing.expectEqual(@as(?u32, 2), channel.pop());
    try testing.expectEqual(@as(?u32, 3), channel.pop());
}

test "channel close wakes consumer" {
    var channel = try Channel(u32).init(testing.allocator, testQueueConfig());
    defer channel.deinit();

    channel.close();
    try testing.expectEqual(@as(?u32, null), channel.pop());
}

test "popBatch drains ready items" {
    var channel = try Channel(u32).init(testing.allocator, .{ .capacity = 4, .strategy = .reject });
    defer channel.deinit();

    try testing.expect(switch (try channel.push(1)) {
        .stored => true,
        else => false,
    });
    try testing.expect(switch (try channel.push(2)) {
        .stored => true,
        else => false,
    });
    try testing.expect(switch (try channel.push(3)) {
        .stored => true,
        else => false,
    });

    var storage: [4]u32 = undefined;
    const count = channel.popBatch(storage[0..]);
    try testing.expectEqual(@as(usize, 3), count);
    try testing.expectEqual(@as(u32, 1), storage[0]);
    try testing.expectEqual(@as(u32, 2), storage[1]);
    try testing.expectEqual(@as(u32, 3), storage[2]);

    channel.close();
    try testing.expectEqual(@as(usize, 0), channel.popBatch(storage[0..]));
}
