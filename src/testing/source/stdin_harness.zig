const std = @import("std");
const source_root = @import("source");
const cfg = source_root.config;
const stdin_impl = source_root.stdin_impl;

pub const SourceSpec = struct {
    lines: []const []const u8,
    mark_eof: bool = true,
};

const SpecEntry = struct {
    id: []const u8,
    spec: SourceSpec,
};

var active_fixture: ?*Fixture = null;

fn create(ctx: source_root.InitContext, config: *const cfg.SourceConfig) source_root.SourceError!source_root.Source {
    const fixture = active_fixture orelse return source_root.SourceError.InvalidConfiguration;
    return fixture.createSource(ctx, config);
}

pub const Fixture = struct {
    allocator: std.mem.Allocator,
    specs: std.ArrayListUnmanaged(SpecEntry),
    factory_storage: [1]source_root.SourceFactory,

    pub fn init(allocator: std.mem.Allocator) Fixture {
        return .{
            .allocator = allocator,
            .specs = .{},
            .factory_storage = .{source_root.SourceFactory{
                .type = .stdin,
                .create = create,
            }},
        };
    }

    pub fn deinit(self: *Fixture) void {
        for (self.specs.items) |entry| {
            self.allocator.free(entry.id);
        }
        self.specs.deinit(self.allocator);
        if (active_fixture == self) {
            active_fixture = null;
        }
    }

    pub fn register(self: *Fixture, id: []const u8, spec: SourceSpec) !void {
        const stored_id = try self.allocator.dupe(u8, id);
        errdefer self.allocator.free(stored_id);
        try self.specs.append(self.allocator, .{
            .id = stored_id,
            .spec = spec,
        });
    }

    pub fn install(self: *Fixture) void {
        active_fixture = self;
    }

    pub fn registry(self: *Fixture) source_root.Registry {
        self.install();
        return .{ .factories = &self.factory_storage };
    }

    fn createSource(self: *Fixture, ctx: source_root.InitContext, config: *const cfg.SourceConfig) source_root.SourceError!source_root.Source {
        const entry = self.findSpec(config.id) orelse return source_root.SourceError.InvalidConfiguration;

        var source_instance = try stdin_impl.factory().create(ctx, config);
        const state = stdin_impl.testing.statePointer(&source_instance);
        const now_ns = state.time_source();

        for (entry.spec.lines) |line| {
            stdin_impl.testing.enqueueLine(state, line, false, now_ns) catch {
                return source_root.SourceError.StartupFailed;
            };
        }

        if (entry.spec.mark_eof) {
            stdin_impl.testing.markEof(state);
        }

        return source_instance;
    }

    fn findSpec(self: *Fixture, id: []const u8) ?SpecEntry {
        for (self.specs.items) |entry| {
            if (std.mem.eql(u8, entry.id, id)) return entry;
        }
        return null;
    }
};
