const std = @import("std");
const event = @import("event.zig");
const cfg = @import("config.zig");
const netx = @import("netx");

pub const SourceError = error{
    InvalidConfiguration,
    StartupFailed,
    ShutdownFailed,
    NotImplemented,
    OperationNotSupported,
    Backpressure,
};

pub const SourceType = enum {
    syslog,
};

pub const SourceDescriptor = struct {
    type: SourceType,
    /// Identifier unique within a running process, surfaced in diagnostics and
    /// metric labels.
    name: []const u8,
};

/// Context made available to concrete sources while they initialize. This keeps
/// the constructor surface extensible without proliferating positional
/// arguments.
pub const InitContext = struct {
    allocator: std.mem.Allocator,
    runtime: *netx.runtime.IoRuntime,
    log: ?*const Logger = null,
    metrics: ?*const Metrics = null,
};

pub const Logger = struct {
    context: *anyopaque,
    write_fn: *const fn (context: *anyopaque, level: LogLevel, message: []const u8) void,

    /// Emits a formatted message at the requested level. Implementations must
    /// be thread-safe when sources operate concurrently.
    pub fn logf(self: *const Logger, level: LogLevel, comptime fmt: []const u8, args: anytype) void {
        var buffer: [512]u8 = undefined;
        const message = std.fmt.bufPrint(&buffer, fmt, args) catch return;
        self.write_fn(self.context, level, message);
    }

    pub fn infof(self: *const Logger, comptime fmt: []const u8, args: anytype) void {
        self.logf(.info, fmt, args);
    }

    pub fn warnf(self: *const Logger, comptime fmt: []const u8, args: anytype) void {
        self.logf(.warn, fmt, args);
    }

    pub fn errorf(self: *const Logger, comptime fmt: []const u8, args: anytype) void {
        self.logf(.err, fmt, args);
    }
};

pub const LogLevel = enum { trace, debug, info, warn, err };

pub const Metrics = struct {
    context: *anyopaque,
    incr_counter_fn: *const fn (context: *anyopaque, name: []const u8, value: u64) void,
    record_gauge_fn: ?*const fn (context: *anyopaque, name: []const u8, value: i64) void = null,

    pub fn incrCounter(self: *const Metrics, name: []const u8, value: u64) void {
        self.incr_counter_fn(self.context, name, value);
    }

    pub fn recordGauge(self: *const Metrics, name: []const u8, value: i64) void {
        const func = self.record_gauge_fn orelse return;
        func(self.context, name, value);
    }
};

pub const Capabilities = struct {
    streaming: bool,
    batching: bool,
};

pub const ReadyObserver = struct {
    context: *anyopaque,
    notify_fn: *const fn (context: *anyopaque) void,

    pub fn notify(self: ReadyObserver) void {
        self.notify_fn(self.context);
    }
};

pub const StreamLifecycle = struct {
    start_stream: *const fn (context: *anyopaque, allocator: std.mem.Allocator) SourceError!?event.EventStream,
    shutdown: *const fn (context: *anyopaque, allocator: std.mem.Allocator) void,
    ready_hint: ?*const fn (context: *anyopaque) bool = null,
    register_ready_observer: ?*const fn (context: *anyopaque, observer: ReadyObserver) void = null,
};

pub const BatchLifecycle = struct {
    poll_batch: *const fn (context: *anyopaque, allocator: std.mem.Allocator) SourceError!?event.EventBatch,
    shutdown: *const fn (context: *anyopaque, allocator: std.mem.Allocator) void,
    ready_hint: ?*const fn (context: *anyopaque) bool = null,
    register_ready_observer: ?*const fn (context: *anyopaque, observer: ReadyObserver) void = null,
};

pub const Source = union(enum) {
    stream: struct {
        descriptor: SourceDescriptor,
        capabilities: Capabilities,
        context: *anyopaque,
        lifecycle: StreamLifecycle,
        batching: union(enum) {
            unsupported,
            supported: BatchLifecycle,
        } = .unsupported,
    },
    batch: struct {
        descriptor: SourceDescriptor,
        capabilities: Capabilities,
        context: *anyopaque,
        lifecycle: BatchLifecycle,
    },

    pub fn descriptor(self: *const Source) SourceDescriptor {
        return switch (self.*) {
            .stream => |stream| stream.descriptor,
            .batch => |batch| batch.descriptor,
        };
    }

    pub fn capabilities(self: *const Source) Capabilities {
        return switch (self.*) {
            .stream => |stream| stream.capabilities,
            .batch => |batch| batch.capabilities,
        };
    }

    pub fn supportsStreaming(self: *const Source) bool {
        return switch (self.*) {
            .stream => true,
            .batch => false,
        };
    }

    pub fn supportsBatching(self: *const Source) bool {
        return switch (self.*) {
            .stream => |stream| switch (stream.batching) {
                .unsupported => false,
                .supported => true,
            },
            .batch => true,
        };
    }

    pub fn startStream(self: *const Source, allocator: std.mem.Allocator) SourceError!?event.EventStream {
        return switch (self.*) {
            .stream => |stream| stream.lifecycle.start_stream(stream.context, allocator),
            .batch => SourceError.OperationNotSupported,
        };
    }

    pub fn pollBatch(self: *const Source, allocator: std.mem.Allocator) SourceError!?event.EventBatch {
        return switch (self.*) {
            .stream => |stream| switch (stream.batching) {
                .unsupported => SourceError.OperationNotSupported,
                .supported => |batch_lc| batch_lc.poll_batch(stream.context, allocator),
            },
            .batch => |batch| batch.lifecycle.poll_batch(batch.context, allocator),
        };
    }

    pub fn shutdown(self: *const Source, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .stream => |stream| stream.lifecycle.shutdown(stream.context, allocator),
            .batch => |batch| batch.lifecycle.shutdown(batch.context, allocator),
        }
    }

    pub fn readyHint(self: *const Source) bool {
        return switch (self.*) {
            .stream => |stream| if (stream.lifecycle.ready_hint) |func| func(stream.context) else true,
            .batch => |batch| if (batch.lifecycle.ready_hint) |func| func(batch.context) else true,
        };
    }

    pub fn registerReadyObserver(self: *const Source, observer: ReadyObserver) void {
        switch (self.*) {
            .stream => |stream| {
                const func = stream.lifecycle.register_ready_observer orelse return;
                func(stream.context, observer);
            },
            .batch => |batch| {
                const func = batch.lifecycle.register_ready_observer orelse return;
                func(batch.context, observer);
            },
        }
    }

    pub fn supportsReadyObserver(self: *const Source) bool {
        return switch (self.*) {
            .stream => |stream| stream.lifecycle.register_ready_observer != null,
            .batch => |batch| batch.lifecycle.register_ready_observer != null,
        };
    }

    pub fn context(self: *const Source) *anyopaque {
        return switch (self.*) {
            .stream => |stream| stream.context,
            .batch => |batch| batch.context,
        };
    }
};

pub const SourceFactory = struct {
    type: SourceType,
    create: *const fn (ctx: InitContext, config: *const cfg.SourceConfig) SourceError!Source,
};
