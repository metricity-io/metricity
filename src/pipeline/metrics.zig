const std = @import("std");
const source_mod = @import("source");
const config_mod = @import("../config/mod.zig");
const event_mod = source_mod.event;

pub const ChannelKind = enum { transform, sink };

pub const ChannelLabels = struct {
    name: []const u8,
    kind: ChannelKind,
};

pub const ChannelObserver = struct {
    context: *anyopaque,
    record_capacity_fn: ?*const fn (context: *anyopaque, capacity: usize) void = null,
    record_depth_fn: ?*const fn (context: *anyopaque, depth: usize) void = null,
    record_drop_fn: ?*const fn (context: *anyopaque, strategy: config_mod.QueueStrategy, count: usize) void = null,
    record_block_fn: ?*const fn (context: *anyopaque, nanoseconds: u64) void = null,

    pub fn recordCapacity(self: ChannelObserver, capacity: usize) void {
        if (self.record_capacity_fn) |func| {
            func(self.context, capacity);
        }
    }

    pub fn recordDepth(self: ChannelObserver, depth: usize) void {
        if (self.record_depth_fn) |func| {
            func(self.context, depth);
        }
    }

    pub fn recordDrop(self: ChannelObserver, strategy: config_mod.QueueStrategy, count: usize) void {
        if (self.record_drop_fn) |func| {
            func(self.context, strategy, count);
        }
    }

    pub fn recordBlock(self: ChannelObserver, nanoseconds: u64) void {
        if (self.record_block_fn) |func| {
            func(self.context, nanoseconds);
        }
    }
};

const ack_metrics = struct {
    const success = "pipeline_ack_success_total";
    const retryable = "pipeline_ack_retryable_total";
    const permanent = "pipeline_ack_permanent_total";
    const latency_total = "pipeline_ack_latency_ns_total";
    const latency_count = "pipeline_ack_latency_events_total";
};

pub const PipelineMetrics = struct {
    sink: ?*const source_mod.Metrics = null,

    pub fn init(sink: ?*const source_mod.Metrics) PipelineMetrics {
        return .{ .sink = sink };
    }

    pub fn createChannelMetrics(
        self: *const PipelineMetrics,
        allocator: std.mem.Allocator,
        labels: ChannelLabels,
    ) !ChannelMetrics {
        var channel_metrics: ChannelMetrics = undefined;
        try channel_metrics.init(allocator, self.sink, labels);
        return channel_metrics;
    }

    pub fn recordAck(self: *const PipelineMetrics, status: event_mod.AckStatus) void {
        const sink = self.sink orelse return;
        const counter_name = switch (status) {
            .success => ack_metrics.success,
            .retryable_failure => ack_metrics.retryable,
            .permanent_failure => ack_metrics.permanent,
        };
        sink.incrCounter(counter_name, 1);
    }

    pub fn recordAckLatency(self: *const PipelineMetrics, latency_ns: u64) void {
        const sink = self.sink orelse return;
        sink.incrCounter(ack_metrics.latency_total, latency_ns);
        sink.incrCounter(ack_metrics.latency_count, 1);
    }
};

pub const ChannelMetrics = struct {
    allocator: std.mem.Allocator,
    sink: ?*const source_mod.Metrics,
    labels: ChannelLabels,

    depth_name: []const u8 = "",
    capacity_name: []const u8 = "",
    drop_newest_name: []const u8 = "",
    drop_oldest_name: []const u8 = "",
    block_total_name: []const u8 = "",
    block_count_name: []const u8 = "",

    pub fn init(self: *ChannelMetrics, allocator: std.mem.Allocator, sink: ?*const source_mod.Metrics, labels: ChannelLabels) !void {
        self.* = .{
            .allocator = allocator,
            .sink = sink,
            .labels = labels,
        };

        if (sink == null) return;

        const kind_str = switch (labels.kind) {
            .transform => "transform",
            .sink => "sink",
        };

        self.depth_name = try allocName(allocator, "pipeline_channel_depth", kind_str, labels.name);
        errdefer allocator.free(self.depth_name);

        self.capacity_name = try allocName(allocator, "pipeline_channel_capacity", kind_str, labels.name);
        errdefer allocator.free(self.capacity_name);

        self.drop_newest_name = try allocName(allocator, "pipeline_channel_drop_newest_total", kind_str, labels.name);
        errdefer allocator.free(self.drop_newest_name);

        self.drop_oldest_name = try allocName(allocator, "pipeline_channel_drop_oldest_total", kind_str, labels.name);
        errdefer allocator.free(self.drop_oldest_name);

        self.block_total_name = try allocName(allocator, "pipeline_channel_push_block_ns_total", kind_str, labels.name);
        errdefer allocator.free(self.block_total_name);

        self.block_count_name = try allocName(allocator, "pipeline_channel_push_block_events_total", kind_str, labels.name);
        errdefer allocator.free(self.block_count_name);
    }

    pub fn deinit(self: *ChannelMetrics) void {
        if (self.sink == null) return;
        self.allocator.free(self.depth_name);
        self.allocator.free(self.capacity_name);
        self.allocator.free(self.drop_newest_name);
        self.allocator.free(self.drop_oldest_name);
        self.allocator.free(self.block_total_name);
        self.allocator.free(self.block_count_name);
        self.* = .{
            .allocator = self.allocator,
            .sink = null,
            .labels = ChannelLabels{ .name = "", .kind = .transform },
        };
    }

    pub fn observer(self: *ChannelMetrics) ChannelObserver {
        return ChannelObserver{
            .context = self,
            .record_capacity_fn = recordCapacity,
            .record_depth_fn = recordDepth,
            .record_drop_fn = recordDrop,
            .record_block_fn = recordBlock,
        };
    }

    fn recordCapacity(context: *anyopaque, capacity: usize) void {
        const self: *ChannelMetrics = @ptrCast(@alignCast(context));
        const sink = self.sink orelse return;
        if (self.capacity_name.len == 0) return;
        if (std.math.cast(i64, capacity)) |value| {
            sink.recordGauge(self.capacity_name, value);
        }
    }

    fn recordDepth(context: *anyopaque, depth: usize) void {
        const self: *ChannelMetrics = @ptrCast(@alignCast(context));
        const sink = self.sink orelse return;
        if (self.depth_name.len == 0) return;
        if (std.math.cast(i64, depth)) |value| {
            sink.recordGauge(self.depth_name, value);
        }
    }

    fn recordDrop(context: *anyopaque, strategy: config_mod.QueueStrategy, count: usize) void {
        const self: *ChannelMetrics = @ptrCast(@alignCast(context));
        const sink = self.sink orelse return;
        const delta: u64 = @intCast(count);
        switch (strategy) {
            .drop_newest => if (self.drop_newest_name.len != 0) sink.incrCounter(self.drop_newest_name, delta),
            .drop_oldest => if (self.drop_oldest_name.len != 0) sink.incrCounter(self.drop_oldest_name, delta),
            .reject => {},
        }
    }

    fn recordBlock(context: *anyopaque, nanoseconds: u64) void {
        if (nanoseconds == 0) return;
        const self: *ChannelMetrics = @ptrCast(@alignCast(context));
        const sink = self.sink orelse return;
        if (self.block_total_name.len != 0) sink.incrCounter(self.block_total_name, nanoseconds);
        if (self.block_count_name.len != 0) sink.incrCounter(self.block_count_name, 1);
    }
};

fn allocName(allocator: std.mem.Allocator, base: []const u8, kind: []const u8, name: []const u8) ![]const u8 {
    return try std.fmt.allocPrint(allocator, "{s}_{s}_{s}", .{ base, kind, name });
}
