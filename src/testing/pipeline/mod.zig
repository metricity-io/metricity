//! Helpers for testing pipeline/collector behaviour (e.g. backpressure).

const std = @import("std");
const metricity = @import("../../root.zig");

const source_mod = metricity.source;
const event_mod = source_mod.event;
const config_mod = metricity.config;
const collector_mod = metricity.collector;

pub const BackpressureScenario = struct {
    retries_before_progress: usize,
    total_events: usize,
};

const BackpressureSource = struct {
    const State = struct {
        retries_remaining: usize,
        events_remaining: usize,
        event_storage: [1]event_mod.Event,
        fields: []event_mod.Field,
        poll_calls: usize = 0,
    };

    pub var scenario: BackpressureScenario = .{ .retries_before_progress = 3, .total_events = 1 };
    pub var last_snapshot: Snapshot = .{ .poll_calls = 0, .events_remaining = 0 };

    pub fn factory() source_mod.SourceFactory {
        return .{ .type = .syslog, .create = create };
    }

    const Snapshot = struct {
        poll_calls: usize,
        events_remaining: usize,
    };

    fn create(ctx: source_mod.InitContext, config: *const source_mod.config.SourceConfig) source_mod.SourceError!source_mod.Source {
        const state = ctx.allocator.create(State) catch return source_mod.SourceError.StartupFailed;
        errdefer ctx.allocator.destroy(state);

        state.* = .{
            .retries_remaining = scenario.retries_before_progress,
            .events_remaining = scenario.total_events,
            .event_storage = undefined,
            .fields = &[_]event_mod.Field{},
        };

        state.fields = ctx.allocator.alloc(event_mod.Field, 1) catch return source_mod.SourceError.StartupFailed;
        errdefer ctx.allocator.free(state.fields);
        state.fields[0] = .{ .name = "value", .value = .{ .integer = 41 } };

        state.event_storage[0] = event_mod.Event{
            .metadata = .{ .source_id = config.id },
            .payload = .{ .log = .{ .message = "hello", .fields = state.fields } },
        };

        const lifecycle = source_mod.BatchLifecycle{
            .poll_batch = pollBatch,
            .shutdown = shutdown,
        };

        return source_mod.Source{
            .batch = .{
                .descriptor = .{ .type = .syslog, .name = config.id },
                .capabilities = .{ .streaming = false, .batching = true },
                .context = state,
                .lifecycle = lifecycle,
            },
        };
    }

    fn startStream(_: *anyopaque, _: std.mem.Allocator) source_mod.SourceError!?event_mod.EventStream {
        return source_mod.SourceError.OperationNotSupported;
    }

    fn pollBatch(context: *anyopaque, _: std.mem.Allocator) source_mod.SourceError!?event_mod.EventBatch {
        const state: *State = @ptrCast(@alignCast(context));
        state.poll_calls += 1;
        if (state.retries_remaining > 0) {
            state.retries_remaining -= 1;
            return source_mod.SourceError.Backpressure;
        }
        if (state.events_remaining == 0) return null;

        state.events_remaining -= 1;
        return event_mod.EventBatch{ .events = state.event_storage[0..1], .ack = event_mod.AckToken.none() };
    }

    fn shutdown(context: *anyopaque, allocator: std.mem.Allocator) void {
        const state: *State = @ptrCast(@alignCast(context));
        last_snapshot = .{ .poll_calls = state.poll_calls, .events_remaining = state.events_remaining };
        allocator.free(state.fields);
        allocator.destroy(state);
    }
};

pub fn runCollectorBackpressureTest(allocator: std.mem.Allocator, scenario: BackpressureScenario) !usize {
    BackpressureSource.scenario = scenario;

    const source_config = source_mod.config.SourceConfig{
        .id = "bp_source",
        .payload = .{ .syslog = .{ .address = "udp://127.0.0.1:0" } },
    };

    var collector = try collector_mod.Collector.init(allocator, &[_]source_mod.config.SourceConfig{source_config}, .{
        .registry = source_mod.Registry{ .factories = &[_]source_mod.SourceFactory{BackpressureSource.factory()} },
    });
    defer collector.deinit();
    defer collector.shutdown(allocator) catch {};

    try collector.start(allocator);

    var polls: usize = 0;
    var delivered: usize = 0;

    while (delivered < scenario.total_events and polls < 10_000) : (polls += 1) {
        const batch = collector.poll(allocator) catch |err| switch (err) {
            collector_mod.CollectorError.Backpressure => continue,
            else => return err,
        };
        if (batch) |value| {
            delivered += value.batch.events.len;
        }
    }

    return delivered;
}

const testing = std.testing;

test "collector recovers from repeated backpressure" {
    const allocator = testing.allocator;
    const delivered = try runCollectorBackpressureTest(allocator, .{ .retries_before_progress = 5, .total_events = 1 });
    try testing.expectEqual(@as(usize, 1), delivered);
    try testing.expectEqual(@as(usize, 0), BackpressureSource.last_snapshot.events_remaining);
    try testing.expectEqual(@as(usize, 6), BackpressureSource.last_snapshot.poll_calls);
}
