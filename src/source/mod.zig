const std = @import("std");

pub const event = @import("event.zig");
pub const config = @import("config.zig");
pub const source = @import("source.zig");
pub const SourceError = source.SourceError;
const syslog = @import("syslog.zig");

const builtin_factories = &[_]source.SourceFactory{
    syslog.factory(),
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


test "builtin registry exposes syslog factory" {
    const registry = Registry.builtin();
    const maybe_factory = registry.findFactory(.syslog);
    try std.testing.expect(maybe_factory != null);
    const factory = maybe_factory.?;
    try std.testing.expectEqual(@as(source.SourceType, .syslog), factory.type);
}
