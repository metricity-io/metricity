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
    streaming: bool = true,
    batching: bool = true,
};

pub const Lifecycle = struct {
    start_stream: *const fn (context: *anyopaque, allocator: std.mem.Allocator) SourceError!?event.EventStream,
    poll_batch: ?*const fn (context: *anyopaque, allocator: std.mem.Allocator) SourceError!?event.EventBatch = null,
    shutdown: *const fn (context: *anyopaque, allocator: std.mem.Allocator) void,
    ready_hint: ?*const fn (context: *anyopaque) bool = null,
};

pub const Source = struct {
    descriptor: SourceDescriptor,
    capabilities: Capabilities,
    lifecycle: Lifecycle,
    context: *anyopaque,

    pub fn startStream(self: *const Source, allocator: std.mem.Allocator) SourceError!?event.EventStream {
        return self.lifecycle.start_stream(self.context, allocator);
    }

    pub fn pollBatch(self: *const Source, allocator: std.mem.Allocator) SourceError!?event.EventBatch {
        const handler = self.lifecycle.poll_batch orelse
            return SourceError.OperationNotSupported;
        return handler(self.context, allocator);
    }

    pub fn shutdown(self: *const Source, allocator: std.mem.Allocator) void {
        self.lifecycle.shutdown(self.context, allocator);
    }

    pub fn readyHint(self: *const Source) bool {
        const func = self.lifecycle.ready_hint orelse return true;
        return func(self.context);
    }
};

pub const SourceFactory = struct {
    type: SourceType,
    create: *const fn (ctx: InitContext, config: *const cfg.SourceConfig) SourceError!Source,
};
