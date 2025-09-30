const std = @import("std");

/// Timestamp represented as nanoseconds since UNIX epoch.
pub const Timestamp = i128;

/// Generic value that can travel with log/metric events. The set purposefully
/// mirrors the SQL literal types supported by the existing parser to ease data
/// interchange between sources and transforms.
pub const Value = union(enum) {
    string: []const u8,
    integer: i64,
    float: f64,
    boolean: bool,
    null,
};

/// Minimal log representation used as the default payload for the SQL-powered
/// pipeline. Extra categories (metrics, traces) can be added in the future by
/// extending `Payload`.
pub const LogEvent = struct {
    message: []const u8,
    fields: []const Field = &[_]Field{},
};

pub const Field = struct {
    name: []const u8,
    value: Value,
};

pub const Payload = union(enum) {
    log: LogEvent,
};

pub const EventMetadata = struct {
    received_at: ?Timestamp = null,
    source_id: ?[]const u8 = null,
    /// Transport-specific metadata (socket address, file offset, etc.).
    transport: ?TransportMetadata = null,
};

pub const TransportMetadata = union(enum) {
    socket: SocketInfo,
    file: FileInfo,
    custom: CustomMetadata,
};

pub const SocketInfo = struct {
    peer_address: []const u8,
    protocol: []const u8,
};

pub const FileInfo = struct {
    path: []const u8,
    offset: u64,
};

pub const CustomMetadata = struct {
    type_name: []const u8,
    opaque_data: []const u8,
};

pub const Event = struct {
    metadata: EventMetadata,
    payload: Payload,
};

pub const EventFinalizerFn = *const fn (context: ?*anyopaque) void;

pub const EventFinalizer = struct {
    function: ?EventFinalizerFn = null,
    context: ?*anyopaque = null,

    pub fn none() EventFinalizer {
        return .{};
    }

    pub fn init(function: EventFinalizerFn, context: ?*anyopaque) EventFinalizer {
        return .{ .function = function, .context = context };
    }

    pub fn run(self: EventFinalizer) void {
        if (self.function) |func| {
            func(self.context);
        }
    }
};

pub const ManagedEvent = struct {
    event: Event,
    finalizer: EventFinalizer = EventFinalizer.none(),

    pub fn fromEvent(event: Event) ManagedEvent {
        return .{ .event = event, .finalizer = EventFinalizer.none() };
    }

    pub fn init(event: Event, finalizer: EventFinalizer) ManagedEvent {
        return .{ .event = event, .finalizer = finalizer };
    }
};

/// A view over a set of events that should be acknowledged together.
pub const AckStatus = enum {
    success,
    retryable_failure,
    permanent_failure,
};

pub const AckError = error{
    MissingHandle,
    AlreadyCompleted,
};

pub const AckHandle = struct {
    context: *anyopaque,
    complete_fn: *const fn (context: *anyopaque, status: AckStatus) void,
    completed: bool = false,

    pub fn init(
        context: *anyopaque,
        complete_fn: *const fn (context: *anyopaque, status: AckStatus) void,
    ) AckHandle {
        return .{ .context = context, .complete_fn = complete_fn };
    }

    pub fn complete(handle: *AckHandle, status: AckStatus) AckError!void {
        if (handle.completed) return AckError.AlreadyCompleted;
        handle.completed = true;
        handle.complete_fn(handle.context, status);
    }
};

pub const AckToken = struct {
    handle: ?*AckHandle = null,

    pub fn none() AckToken {
        return .{ .handle = null };
    }

    pub fn init(handle: *AckHandle) AckToken {
        return .{ .handle = handle };
    }

    pub fn isAvailable(self: AckToken) bool {
        return self.handle != null;
    }

    pub fn success(self: AckToken) AckError!void {
        return self.complete(.success);
    }

    pub fn failPermanent(self: AckToken) AckError!void {
        return self.complete(.permanent_failure);
    }

    pub fn failRetryable(self: AckToken) AckError!void {
        return self.complete(.retryable_failure);
    }

    pub fn complete(self: AckToken, status: AckStatus) AckError!void {
        const handle = self.handle orelse return AckError.MissingHandle;
        try handle.complete(status);
    }
};

pub const EventBatch = struct {
    events: []const Event,
    ack: AckToken = AckToken.none(),
};

pub const StreamError = error{
    EndOfStream,
    TransportFailure,
    DecodeFailure,
    Backpressure,
};

/// Thin vtable used by sources to expose streaming access without committing to
/// a particular concurrency primitive today. Once async/actor infrastructure is
/// introduced we can swap the internals while preserving the public contract.
pub const EventStream = struct {
    context: *anyopaque,
    next_fn: *const fn (context: *anyopaque, allocator: std.mem.Allocator) StreamError!?EventBatch,
    finish_fn: *const fn (context: *anyopaque, allocator: std.mem.Allocator) void,

    pub fn next(self: *EventStream, allocator: std.mem.Allocator) StreamError!?EventBatch {
        return self.next_fn(self.context, allocator);
    }

    pub fn finish(self: *EventStream, allocator: std.mem.Allocator) void {
        self.finish_fn(self.context, allocator);
    }
};

test "ack token completes exactly once" {
    const Context = struct {
        var calls: usize = 0;

        fn handler(_: *anyopaque, status: AckStatus) void {
            std.debug.assert(status == .success);
            calls += 1;
        }
    };

    var handle = AckHandle.init(@constCast(&Context), Context.handler);
    const token = AckToken.init(&handle);

    try token.success();
    try std.testing.expectError(AckError.AlreadyCompleted, token.success());
    try std.testing.expectEqual(@as(usize, 1), Context.calls);
}

test "missing ack handle returns error" {
    const token = AckToken.none();
    try std.testing.expectError(AckError.MissingHandle, token.success());
}
