const std = @import("std");
const xev = @import("xev");
const options = @import("options.zig");
const transport = @import("transport.zig");
const socket_util = @import("socket_util.zig");
const array_list = std.array_list;
const posix = std.posix;

const MessageList = array_list.Managed(transport.Message);
const UdpSocket = xev.UDP;
const UdpState = UdpSocket.State;

fn makeState(userdata: ?*anyopaque) UdpState {
    if (@hasField(UdpState, "op")) {
        return .{ .userdata = userdata, .op = undefined };
    }
    return .{ .userdata = userdata };
}

const UdpContext = struct {
    allocator: std.mem.Allocator,
    loop: *xev.Loop,
    socket: UdpSocket,
    state: UdpState = makeState(null),
    read_completion: xev.Completion = .{},
    scratch: []u8,
    message_limit: usize,
    pending: MessageList,
    pending_head: usize,

    fn init(
        allocator: std.mem.Allocator,
        loop: *xev.Loop,
        opts: options.UdpOptions,
    ) transport.TransportError!*UdpContext {
        const ctx = allocator.create(UdpContext) catch {
            return transport.TransportError.StartupFailed;
        };
        errdefer allocator.destroy(ctx);

        const address = socket_util.resolveAddress(opts.address, "udp://") catch {
            allocator.destroy(ctx);
            return transport.TransportError.StartupFailed;
        };

        var socket = UdpSocket.init(address) catch {
            allocator.destroy(ctx);
            return transport.TransportError.StartupFailed;
        };

        socket_util.configureReuseAddrPort(socket.fd);

        if (socket.bind(address)) |_| {} else |err| {
            socket_util.closeSocket(socket.fd);
            allocator.destroy(ctx);
            std.log.err("udp bind error: {s}", .{@errorName(err)});
            return transport.TransportError.StartupFailed;
        }

        const scratch = allocator.alloc(u8, opts.limits.read_buffer_bytes) catch {
            socket_util.closeSocket(socket.fd);
            allocator.destroy(ctx);
            return transport.TransportError.StartupFailed;
        };

        ctx.* = .{
            .allocator = allocator,
            .loop = loop,
            .socket = socket,
            .scratch = scratch,
            .message_limit = opts.limits.message_size_limit,
            .pending = MessageList.init(allocator),
            .pending_head = 0,
        };

        ctx.state = makeState(@as(*anyopaque, @ptrCast(ctx)));
        ctx.startRead();

        return ctx;
    }

    fn startRead(self: *UdpContext) void {
        self.read_completion = .{};
        self.socket.read(
            self.loop,
            &self.read_completion,
            &self.state,
            .{ .slice = self.scratch },
            UdpContext,
            self,
            readCallback,
        );
    }

    fn deinit(self: *UdpContext) void {
        self.pending.deinit();
        self.allocator.free(self.scratch);
        socket_util.closeSocket(self.socket.fd);
        self.allocator.destroy(self);
    }

    fn takePending(self: *UdpContext) ?transport.Message {
        if (self.pending_head >= self.pending.items.len) return null;
        const message = self.pending.items[self.pending_head];
        self.pending.items[self.pending_head] = undefined;
        self.pending_head += 1;
        if (self.pending_head == self.pending.items.len) {
            self.pending.clearRetainingCapacity();
            self.pending_head = 0;
        }
        return message;
    }

    fn enqueueDatagram(
        self: *UdpContext,
        peer: std.net.Address,
        buffer: []const u8,
    ) void {
        const copy_len = @min(buffer.len, self.message_limit);
        const payload = self.allocator.alloc(u8, copy_len) catch {
            return;
        };
        errdefer self.allocator.free(payload);
        @memcpy(payload, buffer[0..copy_len]);

        var addr_buf: [128]u8 = undefined;
        const peer_slice = socket_util.formatPeerAddress(peer, addr_buf[0..]);
        const peer_copy = self.allocator.alloc(u8, peer_slice.len) catch {
            self.allocator.free(payload);
            return;
        };
        if (peer_slice.len != 0) @memcpy(peer_copy, peer_slice);

        const allocation = transport.IoAllocation.create(self.allocator, payload, peer_copy) catch {
            self.allocator.free(payload);
            self.allocator.free(peer_copy);
            return;
        };

        const truncated = buffer.len > self.message_limit;

        const message = transport.Message{
            .bytes = allocation.payload,
            .metadata = .{
                .protocol = "udp",
                .peer_address = allocation.peer,
                .truncated = truncated,
            },
            .finalizer = transport.IoAllocation.finalizer(allocation),
        };

        self.pending.append(message) catch {
            if (message.finalizer) |finalizer| finalizer.run();
        };
    }
};

fn readCallback(
    ctx_ptr: ?*UdpContext,
    loop: *xev.Loop,
    completion: *xev.Completion,
    state: *UdpState,
    addr: std.net.Address,
    socket: UdpSocket,
    buf: xev.ReadBuffer,
    result: xev.ReadError!usize,
) xev.CallbackAction {
    _ = loop;
    _ = completion;
    _ = state;
    _ = socket;

    const ctx = ctx_ptr.?;
    const len = result catch |err| {
        std.log.err("udp recv error: {s}", .{@errorName(err)});
        return .disarm;
    };

    ctx.enqueueDatagram(addr, buf.slice[0..len]);
    return .rearm;
}

const udp_vtable = transport.VTable{
    .start = udpStart,
    .poll = udpPoll,
    .shutdown = udpShutdown,
};

pub fn create(
    allocator: std.mem.Allocator,
    loop_handle: *xev.Loop,
    opts: options.UdpOptions,
) transport.TransportError!transport.Transport {
    const limits = options.applyBufferDefaults(opts.limits);
    const normalized = options.UdpOptions{
        .address = opts.address,
        .limits = limits,
    };

    const ctx = try UdpContext.init(allocator, loop_handle, normalized);
    errdefer ctx.deinit();

    return transport.Transport.init(ctx, &udp_vtable);
}

fn udpStart(context: *anyopaque) transport.TransportError!void {
    _ = context;
    return;
}

fn udpPoll(context: *anyopaque, allocator: std.mem.Allocator) transport.TransportError!?transport.Message {
    _ = allocator;
    const ctx = getContext(context);

    return ctx.takePending();
}

fn udpShutdown(context: *anyopaque) void {
    const ctx = getContext(context);
    ctx.deinit();
}

fn getContext(ptr: *anyopaque) *UdpContext {
    const aligned: *align(@alignOf(UdpContext)) anyopaque = @alignCast(ptr);
    return @ptrCast(aligned);
}

// removed resolveBindAddress (moved to socket_util)
test "udp enqueueDatagram marks truncated metadata" {
    const allocator = std.testing.allocator;
    var scratch_storage: [0]u8 = .{};
    var ctx = UdpContext{
        .allocator = allocator,
        .loop = undefined,
        .socket = undefined,
        .state = makeState(null),
        .read_completion = .{},
        .scratch = scratch_storage[0..],
        .message_limit = 4,
        .pending = MessageList.init(allocator),
        .pending_head = 0,
    };
    defer ctx.pending.deinit();

    const addr = try std.net.Address.parseIp4("127.0.0.1", 9000);
    ctx.enqueueDatagram(addr, "abcdef");

    const message = (ctx.takePending() orelse unreachable);
    defer if (message.finalizer) |finalizer| finalizer.run();

    try std.testing.expect(message.metadata.truncated);
    try std.testing.expectEqualStrings("abcd", message.bytes);
}
