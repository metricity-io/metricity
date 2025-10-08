const std = @import("std");
const source_mod = @import("source");
const buffer = source_mod.buffer;
const config_mod = @import("../config/mod.zig");
const metrics_mod = @import("metrics.zig");

pub const PushError = error{ WouldBlock, Closed };

pub fn Channel(comptime T: type) type {
    return struct {
        const Buffer = buffer.BoundedBuffer(T);

        mutex: std.Thread.Mutex = .{},
        not_empty: std.Thread.Condition = .{},
        not_full: std.Thread.Condition = .{},
        buffer: Buffer,
        closed: bool = false,
        observer: ?metrics_mod.ChannelObserver = null,

        pub fn init(
            allocator: std.mem.Allocator,
            queue: config_mod.QueueConfig,
            observer: ?metrics_mod.ChannelObserver,
        ) !@This() {
            const when_full = toWhenFull(queue.strategy);
            var channel = @This(){
                .buffer = try Buffer.init(allocator, queue.capacity, when_full),
                .observer = observer,
            };
            if (channel.observer) |obs| {
                obs.recordCapacity(channel.buffer.capacity());
                obs.recordDepth(channel.buffer.len());
            }
            return channel;
        }

        pub fn deinit(self: *@This()) void {
            self.buffer.deinit();
            self.* = undefined;
        }

        pub fn drainWith(self: *@This(), handler: anytype) void {
            self.mutex.lock();
            defer self.mutex.unlock();

            while (self.buffer.pop()) |item| {
                handler(item);
            }

            if (self.observer) |obs| {
                obs.recordDepth(self.buffer.len());
            }
            self.not_full.broadcast();
        }

        pub fn deinitWith(self: *@This(), handler: anytype) void {
            self.drainWith(handler);
            self.deinit();
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
                    if (self.observer) |obs| {
                        obs.recordDepth(self.buffer.len());
                    }
                    return PushResult(T){ .stored = {} };
                },
                .dropped_newest => {
                    if (self.observer) |obs| obs.recordDrop(.drop_newest, 1);
                    return PushResult(T){ .dropped_newest = {} };
                },
                .dropped_oldest => |evicted| {
                    self.not_empty.signal();
                    if (self.observer) |obs| {
                        obs.recordDrop(.drop_oldest, 1);
                        obs.recordDepth(self.buffer.len());
                    }
                    return PushResult(T){ .dropped_oldest = evicted };
                },
            }
        }

        pub fn pushBlocking(self: *@This(), item: T) PushError!PushResult(T) {
            self.mutex.lock();
            defer self.mutex.unlock();

            var blocked_ns: u64 = 0;
            while (true) {
                if (self.closed) return PushError.Closed;

                const outcome = self.buffer.push(item) catch |err| switch (err) {
                    buffer.PushError.WouldBlock => {
                        const wait_start = std.time.nanoTimestamp();
                        self.not_full.wait(&self.mutex);
                        const wait_end = std.time.nanoTimestamp();
                        blocked_ns += @intCast(wait_end - wait_start);
                        continue;
                    },
                };

                switch (outcome) {
                    .stored => {
                        self.not_empty.signal();
                        if (self.observer) |obs| {
                            obs.recordDepth(self.buffer.len());
                            obs.recordBlock(blocked_ns);
                        }
                        return PushResult(T){ .stored = {} };
                    },
                    .dropped_newest => {
                        if (self.observer) |obs| {
                            obs.recordDrop(.drop_newest, 1);
                            obs.recordBlock(blocked_ns);
                        }
                        return PushResult(T){ .dropped_newest = {} };
                    },
                    .dropped_oldest => |evicted| {
                        self.not_empty.signal();
                        if (self.observer) |obs| {
                            obs.recordDrop(.drop_oldest, 1);
                            obs.recordDepth(self.buffer.len());
                            obs.recordBlock(blocked_ns);
                        }
                        return PushResult(T){ .dropped_oldest = evicted };
                    },
                }
            }
        }

        pub fn pop(self: *@This()) ?T {
            self.mutex.lock();
            defer self.mutex.unlock();

            while (self.buffer.len() == 0) {
                if (self.closed) return null;
                self.not_empty.wait(&self.mutex);
            }

            const item = self.buffer.pop() orelse unreachable;
            if (self.observer) |obs| {
                obs.recordDepth(self.buffer.len());
            }
            self.not_full.signal();
            return item;
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
            if (count > 0) {
                self.not_full.broadcast();
                if (self.observer) |obs| {
                    obs.recordDepth(self.buffer.len());
                }
            }
            return count;
        }

        pub fn close(self: *@This()) void {
            self.mutex.lock();
            defer self.mutex.unlock();
            if (self.closed) return;
            self.closed = true;
            self.not_empty.broadcast();
            self.not_full.broadcast();
        }

        pub fn clearObserver(self: *@This()) void {
            self.mutex.lock();
            self.observer = null;
            self.mutex.unlock();
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
    var channel = try Channel(u32).init(testing.allocator, testQueueConfig(), null);
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
    var channel = try Channel(u32).init(testing.allocator, .{ .capacity = 2, .strategy = .drop_oldest }, null);
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
    var channel = try Channel(u32).init(testing.allocator, testQueueConfig(), null);
    defer channel.deinit();

    channel.close();
    try testing.expectEqual(@as(?u32, null), channel.pop());
}

test "popBatch drains ready items" {
    var channel = try Channel(u32).init(testing.allocator, .{ .capacity = 4, .strategy = .reject }, null);
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

test "channel metrics capture depth and drops" {
    const allocator = testing.allocator;

    const Recorder = struct {
        depth_name: []const u8 = "",
        capacity_name: []const u8 = "",
        drop_newest_name: []const u8 = "",
        drop_oldest_name: []const u8 = "",
        block_total_name: []const u8 = "",
        block_count_name: []const u8 = "",

        last_depth: ?i64 = null,
        capacity: ?i64 = null,
        drop_newest: u64 = 0,
        drop_oldest: u64 = 0,
        block_total: u64 = 0,
        block_events: u64 = 0,

        fn incr(context: *anyopaque, name: []const u8, value: u64) void {
            const self: *@This() = @ptrCast(@alignCast(context));
            if (self.drop_newest_name.len != 0 and std.mem.eql(u8, name, self.drop_newest_name)) {
                self.drop_newest += value;
                return;
            }
            if (self.drop_oldest_name.len != 0 and std.mem.eql(u8, name, self.drop_oldest_name)) {
                self.drop_oldest += value;
                return;
            }
            if (self.block_total_name.len != 0 and std.mem.eql(u8, name, self.block_total_name)) {
                self.block_total += value;
                return;
            }
            if (self.block_count_name.len != 0 and std.mem.eql(u8, name, self.block_count_name)) {
                self.block_events += value;
                return;
            }
        }

        fn gauge(context: *anyopaque, name: []const u8, value: i64) void {
            const self: *@This() = @ptrCast(@alignCast(context));
            if (self.capacity_name.len != 0 and std.mem.eql(u8, name, self.capacity_name)) {
                self.capacity = value;
                return;
            }
            if (self.depth_name.len != 0 and std.mem.eql(u8, name, self.depth_name)) {
                self.last_depth = value;
                return;
            }
        }
    };

    var recorder = Recorder{};
    const metrics_sink = source_mod.Metrics{
        .context = &recorder,
        .incr_counter_fn = Recorder.incr,
        .record_gauge_fn = Recorder.gauge,
    };

    var pipeline_metrics = metrics_mod.PipelineMetrics.init(&metrics_sink);

    var transform_metrics = try pipeline_metrics.createChannelMetrics(allocator, .{ .name = "metrics_test", .kind = .transform });
    defer transform_metrics.deinit();

    recorder.depth_name = transform_metrics.depth_name;
    recorder.capacity_name = transform_metrics.capacity_name;
    recorder.drop_oldest_name = transform_metrics.drop_oldest_name;
    recorder.block_total_name = transform_metrics.block_total_name;
    recorder.block_count_name = transform_metrics.block_count_name;

    var channel = try Channel(u32).init(allocator, .{ .capacity = 2, .strategy = .drop_oldest }, transform_metrics.observer());
    defer channel.deinit();

    try testing.expectEqual(@as(?i64, 2), recorder.capacity);
    try testing.expectEqual(@as(?i64, 0), recorder.last_depth);

    try testing.expect(switch (try channel.push(1)) {
        .stored => true,
        else => false,
    });
    try testing.expectEqual(@as(?i64, 1), recorder.last_depth);

    try testing.expect(switch (try channel.push(2)) {
        .stored => true,
        else => false,
    });
    try testing.expectEqual(@as(?i64, 2), recorder.last_depth);

    const oldest_outcome = try channel.push(3);
    try testing.expect(oldest_outcome == .dropped_oldest);
    try testing.expectEqual(@as(u64, 1), recorder.drop_oldest);
    try testing.expectEqual(@as(?i64, 2), recorder.last_depth);

    _ = channel.pop();
    try testing.expectEqual(@as(?i64, 1), recorder.last_depth);

    var newest_metrics = try pipeline_metrics.createChannelMetrics(allocator, .{ .name = "drop_newest", .kind = .transform });
    defer newest_metrics.deinit();
    recorder.drop_newest_name = newest_metrics.drop_newest_name;

    var newest_channel = try Channel(u32).init(allocator, .{ .capacity = 1, .strategy = .drop_newest }, newest_metrics.observer());
    defer newest_channel.deinit();
    try testing.expect(switch (try newest_channel.push(11)) {
        .stored => true,
        else => false,
    });
    const newest_outcome = try newest_channel.push(22);
    try testing.expect(newest_outcome == .dropped_newest);
    try testing.expectEqual(@as(u64, 1), recorder.drop_newest);

    channel.close();
    newest_channel.close();
    while (channel.pop()) |_| {}
    while (newest_channel.pop()) |_| {}
}

test "pushBlocking waits for capacity" {
    const atomic_order = std.builtin.AtomicOrder;
    const allocator = testing.allocator;

    const Recorder = struct {
        depth_name: []const u8 = "",
        capacity_name: []const u8 = "",
        block_total_name: []const u8 = "",
        block_count_name: []const u8 = "",

        block_total: u64 = 0,
        block_events: u64 = 0,

        fn incr(context: *anyopaque, name: []const u8, value: u64) void {
            const self: *@This() = @ptrCast(@alignCast(context));
            if (self.block_total_name.len != 0 and std.mem.eql(u8, name, self.block_total_name)) {
                self.block_total += value;
                return;
            }
            if (self.block_count_name.len != 0 and std.mem.eql(u8, name, self.block_count_name)) {
                self.block_events += value;
                return;
            }
        }

        fn gauge(context: *anyopaque, _: []const u8, _: i64) void {
            _ = context;
        }
    };

    var recorder = Recorder{};
    const metrics_sink = source_mod.Metrics{
        .context = &recorder,
        .incr_counter_fn = Recorder.incr,
        .record_gauge_fn = Recorder.gauge,
    };

    var pipeline_metrics = metrics_mod.PipelineMetrics.init(&metrics_sink);
    var channel_metrics = try pipeline_metrics.createChannelMetrics(allocator, .{ .name = "blocking", .kind = .transform });
    defer channel_metrics.deinit();
    recorder.block_total_name = channel_metrics.block_total_name;
    recorder.block_count_name = channel_metrics.block_count_name;

    var channel = try Channel(u32).init(allocator, .{ .capacity = 1, .strategy = .reject }, channel_metrics.observer());
    defer channel.deinit();

    try testing.expect(switch (try channel.push(1)) {
        .stored => true,
        else => false,
    });

    const Worker = struct {
        channel: *Channel(u32),
        done: *std.atomic.Value(bool),

        fn run(self: *@This()) void {
            const outcome = self.channel.pushBlocking(2) catch return;
            switch (outcome) {
                .stored => {},
                .dropped_newest, .dropped_oldest => unreachable,
            }
            self.done.store(true, atomic_order.seq_cst);
        }
    };

    var done = std.atomic.Value(bool).init(false);
    var worker = Worker{ .channel = &channel, .done = &done };
    const thread = try std.Thread.spawn(.{}, Worker.run, .{&worker});

    std.Thread.sleep(5 * std.time.ns_per_ms);
    try testing.expect(!done.load(atomic_order.seq_cst));

    try testing.expectEqual(@as(?u32, 1), channel.pop());
    thread.join();

    try testing.expect(done.load(atomic_order.seq_cst));
    try testing.expectEqual(@as(?u32, 2), channel.pop());
    try testing.expectEqual(@as(u64, 1), recorder.block_events);
    try testing.expect(recorder.block_total > 0);
}
