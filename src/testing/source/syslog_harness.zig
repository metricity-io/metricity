const std = @import("std");
const source_root = @import("source");
const cfg = source_root.config;
const syslog_testing = source_root.testing.syslog;
const netx = @import("netx");
const netx_transport = netx.transport;

pub const SourceSpec = struct {
    frames: []const []const u8,
    peer_address: []const u8 = "127.0.0.1:0",
    protocol: []const u8 = "udp",
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

const TransportFeed = struct {
    allocator: std.mem.Allocator,
    frames: []const []const u8,
    index: usize = 0,
    peer_address: []const u8,
    protocol: []const u8,

    fn init(
        allocator: std.mem.Allocator,
        frames: []const []const u8,
        peer_address: []const u8,
        protocol: []const u8,
    ) TransportFeed {
        return .{
            .allocator = allocator,
            .frames = frames,
            .index = 0,
            .peer_address = peer_address,
            .protocol = protocol,
        };
    }

    fn dispose(self: *TransportFeed) void {
        self.allocator.destroy(self);
    }

    fn get(context: *anyopaque) *TransportFeed {
        const aligned: *align(@alignOf(TransportFeed)) anyopaque = @alignCast(context);
        return @ptrCast(aligned);
    }

    fn start(_: *anyopaque) netx_transport.TransportError!void {
        return;
    }

    fn poll(context: *anyopaque, _: std.mem.Allocator) netx_transport.TransportError!?netx_transport.Message {
        const self = get(context);
        if (self.index >= self.frames.len) return null;

        const payload = self.frames[self.index];
        self.index += 1;

        return netx_transport.Message{
            .bytes = payload,
            .metadata = .{
                .peer_address = self.peer_address,
                .protocol = self.protocol,
                .truncated = false,
            },
            .finalizer = null,
        };
    }

    fn shutdown(context: *anyopaque) void {
        const self = get(context);
        self.dispose();
    }

    const vtable = netx_transport.VTable{
        .start = start,
        .poll = poll,
        .shutdown = shutdown,
    };
};

pub const Fixture = struct {
    allocator: std.mem.Allocator,
    specs: std.ArrayListUnmanaged(SpecEntry),
    factory_storage: [1]source_root.SourceFactory,

    pub fn init(allocator: std.mem.Allocator) Fixture {
        return .{
            .allocator = allocator,
            .specs = .{},
            .factory_storage = .{source_root.SourceFactory{
                .type = .syslog,
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

        const feed_ptr = ctx.allocator.create(TransportFeed) catch {
            return source_root.SourceError.StartupFailed;
        };
        errdefer feed_ptr.dispose();

        feed_ptr.* = TransportFeed.init(ctx.allocator, entry.spec.frames, entry.spec.peer_address, entry.spec.protocol);
        const transport = netx_transport.Transport.init(@as(*anyopaque, @ptrCast(feed_ptr)), &TransportFeed.vtable);

        const options = syslog_testing.CreateOptions{};
        return syslog_testing.createWithTransport(ctx, config, transport, options);
    }

    fn findSpec(self: *Fixture, id: []const u8) ?SpecEntry {
        for (self.specs.items) |entry| {
            if (std.mem.eql(u8, entry.id, id)) return entry;
        }
        return null;
    }
};
