const std = @import("std");

pub const TransportError = error{
    StartupFailed,
    ShutdownFailed,
    Io,
    NotImplemented,
};

pub const Metadata = struct {
    peer_address: []const u8 = "",
    protocol: []const u8 = "",
    truncated: bool = false,
};

pub const Finalizer = struct {
    context: *anyopaque,
    function: *const fn (context: *anyopaque) void,

    pub fn run(self: Finalizer) void {
        self.function(self.context);
    }
};

pub const Message = struct {
    bytes: []const u8,
    metadata: Metadata,
    finalizer: ?Finalizer = null,
};

pub const VTable = struct {
    start: *const fn (context: *anyopaque) TransportError!void,
    poll: *const fn (context: *anyopaque, allocator: std.mem.Allocator) TransportError!?Message,
    shutdown: *const fn (context: *anyopaque) void,
};

pub const Transport = struct {
    context: *anyopaque,
    vtable: *const VTable,

    pub fn init(context: *anyopaque, vtable: *const VTable) Transport {
        return .{ .context = context, .vtable = vtable };
    }

    pub fn start(self: *Transport) TransportError!void {
        return self.vtable.start(self.context);
    }

    pub fn poll(self: *Transport, allocator: std.mem.Allocator) TransportError!?Message {
        return self.vtable.poll(self.context, allocator);
    }

    pub fn shutdown(self: *Transport) void {
        self.vtable.shutdown(self.context);
    }
};

/// Lifecycle guard for payload + peer buffers produced by transports.
pub const IoAllocation = struct {
    allocator: std.mem.Allocator,
    payload: []u8,
    peer: []u8,

    pub fn create(
        allocator: std.mem.Allocator,
        payload: []u8,
        peer: []u8,
    ) !*IoAllocation {
        const allocation = try allocator.create(IoAllocation);
        allocation.* = .{
            .allocator = allocator,
            .payload = payload,
            .peer = peer,
        };
        return allocation;
    }

    pub fn finalizer(allocation: *IoAllocation) Finalizer {
        return Finalizer{
            .context = allocation,
            .function = IoAllocation.release,
        };
    }

    fn release(context: *anyopaque) void {
        const aligned: *align(@alignOf(IoAllocation)) anyopaque = @alignCast(context);
        const allocation: *IoAllocation = @ptrCast(aligned);
        allocation.allocator.free(allocation.payload);
        allocation.allocator.free(allocation.peer);
        allocation.allocator.destroy(allocation);
    }
};
