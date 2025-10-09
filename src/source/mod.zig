const std = @import("std");

pub const event = @import("event.zig");
pub const config = @import("config.zig");
pub const buffer = @import("buffer.zig");
pub const syslog_frame = @import("syslog/frame.zig");
pub const syslog_parser = @import("syslog/parser.zig");
const source_impl = @import("source.zig");
pub const source = source_impl;
pub const SourceError = source_impl.SourceError;
pub const SourceType = source_impl.SourceType;
pub const SourceDescriptor = source_impl.SourceDescriptor;
pub const Source = source_impl.Source;
pub const SourceFactory = source_impl.SourceFactory;
pub const InitContext = source_impl.InitContext;
pub const Logger = source_impl.Logger;
pub const Metrics = source_impl.Metrics;
pub const Capabilities = source_impl.Capabilities;
pub const StreamLifecycle = source_impl.StreamLifecycle;
pub const BatchLifecycle = source_impl.BatchLifecycle;
pub const ReadyObserver = source_impl.ReadyObserver;
const syslog_impl = @import("syslog.zig");

const builtin_factories = &[_]source.SourceFactory{
    syslog_impl.factory(),
};

pub const Registry = struct {
    factories: []const source.SourceFactory,

    pub fn builtin() Registry {
        return .{ .factories = builtin_factories };
    }

    pub fn findFactory(self: Registry, kind: source.SourceType) ?source.SourceFactory {
        for (self.factories) |factory| {
            if (factory.type == kind) return factory;
        }
        return null;
    }

    pub fn iter(self: Registry) []const source.SourceFactory {
        return self.factories;
    }
};

/// Temporary function signalling that the source module is still under
/// construction. This can be used by integration tests to assert the module
/// is wired correctly before real implementations exist.
pub fn status() []const u8 {
    return "source module initialized";
}

pub const testing = struct {
    pub const syslog = syslog_impl.testing;
};

test "builtin registry exposes syslog factory" {
    const registry = Registry.builtin();
    const maybe_factory = registry.findFactory(.syslog);
    try std.testing.expect(maybe_factory != null);
    const factory = maybe_factory.?; 
    try std.testing.expectEqual(@as(source.SourceType, .syslog), factory.type);
}
