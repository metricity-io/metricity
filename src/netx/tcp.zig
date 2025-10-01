const std = @import("std");
const xev = @import("xev");
const options = @import("options.zig");
const ring_buffer = @import("ring_buffer.zig");
const transport = @import("transport.zig");
const socket_util = @import("socket_util.zig");
const posix = std.posix;
const array_list = std.array_list;

const MessageList = array_list.Managed(transport.Message);
const ConnectionList = array_list.Managed(*Connection);

const ProcessError = error{
    ConnectionClosed,
    Fatal,
};

const TcpContext = struct {
    allocator: std.mem.Allocator,
    loop: *xev.Loop,
    listener: xev.TCP,
    accept_completion: xev.Completion = .{},
    scratch: []u8,
    connections: ConnectionList,
    pending: MessageList,
    pending_head: usize,
    read_buffer_bytes: usize,
    message_limit: usize,
    max_connections: usize,
    keepalive_seconds: ?u32,
    high_watermark: usize,
    low_watermark: usize,
    shutting_down: bool = false,

    fn init(
        allocator: std.mem.Allocator,
        loop: *xev.Loop,
        opts: options.TcpOptions,
    ) transport.TransportError!*TcpContext {
        const ctx = allocator.create(TcpContext) catch {
            return transport.TransportError.StartupFailed;
        };
        errdefer allocator.destroy(ctx);

        const address = socket_util.resolveAddress(opts.address, "tcp://") catch {
            allocator.destroy(ctx);
            return transport.TransportError.StartupFailed;
        };

        var socket = xev.TCP.init(address) catch {
            allocator.destroy(ctx);
            return transport.TransportError.StartupFailed;
        };

        socket_util.configureReuseAddr(socket.fd);

        if (socket.bind(address)) |_| {} else |err| {
            posix.close(socket.fd) catch {};
            allocator.destroy(ctx);
            std.log.err("tcp bind error: {s}", .{@errorName(err)});
            return transport.TransportError.StartupFailed;
        }

        if (socket.listen(@intCast(posix.SOMAXCONN))) |_| {} else |err| {
            posix.close(socket.fd) catch {};
            allocator.destroy(ctx);
            std.log.err("tcp listen error: {s}", .{@errorName(err)});
            return transport.TransportError.StartupFailed;
        }

        const scratch = allocator.alloc(u8, opts.limits.read_buffer_bytes) catch {
            posix.close(socket.fd) catch {};
            allocator.destroy(ctx);
            return transport.TransportError.StartupFailed;
        };

        ctx.* = .{
            .allocator = allocator,
            .loop = loop,
            .listener = socket,
            .scratch = scratch,
            .connections = ConnectionList.init(allocator),
            .pending = MessageList.init(allocator),
            .pending_head = 0,
            .read_buffer_bytes = opts.limits.read_buffer_bytes,
            .message_limit = opts.limits.message_size_limit,
            .max_connections = opts.max_connections,
            .keepalive_seconds = opts.keepalive_seconds,
            .high_watermark = opts.high_watermark,
            .low_watermark = opts.low_watermark,
        };

        ctx.accept_completion = .{};

        return ctx;
    }

    fn deinit(self: *TcpContext) void {
        self.shutdownConnections();
        self.pending.deinit();
        self.connections.deinit();
        self.allocator.free(self.scratch);
        posix.close(self.listener.fd) catch {};
        self.allocator.destroy(self);
    }

    fn takePending(self: *TcpContext) ?transport.Message {
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

    fn registerConnection(self: *TcpContext, loop: *xev.Loop, socket: xev.TCP) void {
        if (self.connections.items.len >= self.max_connections) {
            closeSocketImmediate(socket);
            return;
        }

        const conn = self.allocator.create(Connection) catch {
            closeSocketImmediate(socket);
            return;
        };
        conn.* = .{
            .parent = self,
            .socket = socket,
            .fd = socket.fd,
            .ring = undefined,
            .peer_address = &[_]u8{},
            .poll_completion = .{},
            .suspended = false,
        };

        const ring_limit = self.ringMaxCapacity();
        conn.ring = ring_buffer.RingBuffer.initWithLimit(self.allocator, self.read_buffer_bytes, ring_limit) catch {
            self.allocator.destroy(conn);
            closeSocketImmediate(socket);
            return;
        };

        conn.peer_address = socket_util.fetchPeerAddress(self.allocator, socket) catch {
            conn.ring.deinit();
            self.allocator.destroy(conn);
            closeSocketImmediate(socket);
            return;
        };

        if (self.keepalive_seconds) |seconds| {
            socket_util.configureTcpKeepalive(conn.fd, seconds);
        }

        self.connections.append(conn) catch {
            conn.ring.deinit();
            self.allocator.free(conn.peer_address);
            self.allocator.destroy(conn);
            closeSocketImmediate(socket);
            return;
        };

        conn.startPoll(loop);
    }

    fn ringMaxCapacity(self: *const TcpContext) ?usize {
        if (self.high_watermark == std.math.maxInt(usize)) return null;

        var limit = self.high_watermark;
        const doubled = std.math.mul(usize, self.high_watermark, 2) catch std.math.maxInt(usize);
        if (doubled > limit) {
            limit = doubled;
        }
        if (limit < self.read_buffer_bytes) {
            limit = self.read_buffer_bytes;
        }
        return limit;
    }

    fn processConnection(self: *TcpContext, conn: *Connection) ProcessError!void {
        while (true) {
            const read_len = posix.recv(conn.fd, self.scratch, posix.MSG.DONTWAIT) catch |err| switch (err) {
                error.WouldBlock => break,
                error.ConnectionResetByPeer, error.SocketNotConnected => return error.ConnectionClosed,
                error.SystemResources => return error.Fatal,
                else => return error.Fatal,
            };

            if (read_len == 0) return error.ConnectionClosed;

            conn.ring.write(self.scratch[0..read_len]) catch {
                return error.Fatal;
            };
        }

        try self.flushBufferedChunks(conn);
    }

    fn flushBufferedChunks(self: *TcpContext, conn: *Connection) ProcessError!void {
        while (conn.ring.len() > 0) {
            const available = conn.ring.len();
            const take = @min(available, self.message_limit);
            const payload = try self.allocator.alloc(u8, take);
            errdefer self.allocator.free(payload);

            conn.ring.copyOut(payload);
            conn.ring.consume(take);

            const peer_copy = try socket_util.copySlice(self.allocator, conn.peer_address);
            const truncated = available > self.message_limit;

            const allocation = transport.IoAllocation.create(self.allocator, payload, peer_copy) catch {
                self.allocator.free(payload);
                self.allocator.free(peer_copy);
                return error.Fatal;
            };

            const message = transport.Message{
                .bytes = allocation.payload,
                .metadata = .{
                    .protocol = "tcp",
                    .peer_address = allocation.peer,
                    .truncated = truncated,
                },
                .finalizer = transport.IoAllocation.finalizer(allocation),
            };

            self.pending.append(message) catch {
                if (message.finalizer) |finalizer| finalizer.run();
                return error.Fatal;
            };

            _ = self.maybeResume(conn);
        }
    }

    fn maybeResume(self: *TcpContext, conn: *Connection) bool {
        if (!conn.suspended) return false;
        if (conn.ring.len() >= self.low_watermark) return false;
        conn.startPoll(self.loop);
        return true;
    }

    fn shouldThrottle(self: *TcpContext, conn: *Connection) bool {
        return conn.ring.len() >= self.high_watermark;
    }

    fn removeConnection(self: *TcpContext, target: *Connection) void {
        var index: usize = 0;
        while (index < self.connections.items.len) : (index += 1) {
            if (self.connections.items[index] == target) {
                _ = self.connections.swapRemove(index);
                break;
            }
        }
    }

    fn shutdownConnections(self: *TcpContext) void {
        self.shutting_down = true;
        while (self.connections.items.len > 0) {
            const conn = self.connections.pop();
            conn.deinit();
        }
    }
};

const Connection = struct {
    parent: *TcpContext,
    socket: xev.TCP,
    fd: posix.socket_t,
    ring: ring_buffer.RingBuffer,
    peer_address: []u8,
    poll_completion: xev.Completion = .{},
    suspended: bool,

    fn startPoll(self: *Connection, loop: *xev.Loop) void {
        self.poll_completion = .{};
        self.socket.poll(loop, &self.poll_completion, xev.PollEvent.read, Connection, self, pollCallback);
        self.suspended = false;
    }

    fn markSuspended(self: *Connection) void {
        self.suspended = true;
    }

    fn deinit(self: *Connection) void {
        posix.close(self.fd) catch {};
        self.ring.deinit();
        self.parent.allocator.free(self.peer_address);
        self.parent.allocator.destroy(self);
    }
};

fn pollCallback(
    conn_ptr: ?*Connection,
    loop: *xev.Loop,
    completion: *xev.Completion,
    socket: xev.TCP,
    result: xev.PollError!xev.PollEvent,
) xev.CallbackAction {
    const conn = conn_ptr.?;
    const ctx = conn.parent;
    _ = loop;
    _ = completion;
    _ = socket;

    if (result) |_| {
        const process = ctx.processConnection(conn);
        if (process) |_| {} else |err| switch (err) {
            error.ConnectionClosed => {
                ctx.removeConnection(conn);
                conn.deinit();
                return .disarm;
            },
            error.Fatal => {
                ctx.removeConnection(conn);
                conn.deinit();
                return .disarm;
            },
        }

        if (ctx.shouldThrottle(conn)) {
            conn.markSuspended();
            return if (ctx.maybeResume(conn)) .rearm else .disarm;
        }

        if (ctx.shutting_down) {
            ctx.removeConnection(conn);
            conn.deinit();
            return .disarm;
        }

        return .rearm;
    } else |err| {
        std.log.err("tcp poll error: {s}", .{@errorName(err)});
        ctx.removeConnection(conn);
        conn.deinit();
        return .disarm;
    }
}

fn acceptCallback(
    ctx_ptr: ?*TcpContext,
    loop: *xev.Loop,
    completion: *xev.Completion,
    result: xev.AcceptError!xev.TCP,
) xev.CallbackAction {
    const ctx = ctx_ptr.?;
    if (ctx.shutting_down) {
        if (result) |sock| {
            closeSocketImmediate(sock);
        } else |_| {}
        return .disarm;
    }

    ctx.listener.accept(loop, completion, TcpContext, ctx, acceptCallback);

    const socket = result catch |err| {
        std.log.err("tcp accept error: {s}", .{@errorName(err)});
        return .disarm;
    };

    ctx.registerConnection(loop, socket);
    return .disarm;
}

fn closeSocketImmediate(socket: xev.TCP) void {
    const fd = socket.fd;
    posix.close(fd) catch {};
}

fn ring_bufferCleanup(ring: ring_buffer.RingBuffer, allocator: std.mem.Allocator) void {
    var r = ring;
    r.deinit();
    _ = allocator;
}

fn tcpStart(context: *anyopaque) transport.TransportError!void {
    _ = context;
    return;
}

fn tcpPoll(context: *anyopaque, allocator: std.mem.Allocator) transport.TransportError!?transport.Message {
    _ = allocator;
    const ctx = getContext(context);

    return ctx.takePending();
}

fn tcpShutdown(context: *anyopaque) void {
    const ctx = getContext(context);
    ctx.deinit();
}

fn getContext(ptr: *anyopaque) *TcpContext {
    const aligned: *align(@alignOf(TcpContext)) anyopaque = @alignCast(ptr);
    return @ptrCast(aligned);
}

test "flushBufferedChunks splits payload into capped messages" {
    const allocator = std.testing.allocator;
    var scratch_storage: [0]u8 = .{};
    var ctx = TcpContext{
        .allocator = allocator,
        .loop = undefined,
        .listener = undefined,
        .accept_completion = .{},
        .scratch = scratch_storage[0..],
        .connections = ConnectionList.init(allocator),
        .pending = MessageList.init(allocator),
        .pending_head = 0,
        .read_buffer_bytes = 0,
        .message_limit = 4,
        .max_connections = 0,
        .keepalive_seconds = null,
        .high_watermark = 8,
        .low_watermark = 4,
        .shutting_down = false,
    };
    defer ctx.connections.deinit();
    defer ctx.pending.deinit();

    var empty_peer: [0]u8 = .{};
    var conn = Connection{
        .parent = &ctx,
        .socket = undefined,
        .fd = 0,
        .ring = undefined,
        .peer_address = empty_peer[0..],
        .poll_completion = .{},
        .suspended = false,
    };
    conn.ring = try ring_buffer.RingBuffer.initWithLimit(allocator, 8, 16);
    defer conn.ring.deinit();

    try conn.ring.write("abcdef");
    try ctx.flushBufferedChunks(&conn);

    const first = ctx.takePending() orelse unreachable;
    defer if (first.finalizer) |finalizer| finalizer.run();
    try std.testing.expectEqualStrings("abcd", first.bytes);
    try std.testing.expect(first.metadata.truncated);

    const second = ctx.takePending() orelse unreachable;
    defer if (second.finalizer) |finalizer| finalizer.run();
    try std.testing.expectEqualStrings("ef", second.bytes);
    try std.testing.expect(!second.metadata.truncated);

    try std.testing.expect(ctx.takePending() == null);
}

test "flushBufferedChunks no-op on empty buffer" {
    const allocator = std.testing.allocator;
    var scratch_storage: [0]u8 = .{};
    var ctx = TcpContext{
        .allocator = allocator,
        .loop = undefined,
        .listener = undefined,
        .accept_completion = .{},
        .scratch = scratch_storage[0..],
        .connections = ConnectionList.init(allocator),
        .pending = MessageList.init(allocator),
        .pending_head = 0,
        .read_buffer_bytes = 0,
        .message_limit = 8,
        .max_connections = 0,
        .keepalive_seconds = null,
        .high_watermark = std.math.maxInt(usize),
        .low_watermark = std.math.maxInt(usize),
        .shutting_down = false,
    };
    defer ctx.connections.deinit();
    defer ctx.pending.deinit();

    var empty_peer: [0]u8 = .{};
    var conn = Connection{
        .parent = &ctx,
        .socket = undefined,
        .fd = 0,
        .ring = try ring_buffer.RingBuffer.init(allocator, 8),
        .peer_address = empty_peer[0..],
        .poll_completion = .{},
        .suspended = false,
    };
    defer conn.ring.deinit();

    try ctx.flushBufferedChunks(&conn);
    try std.testing.expect(ctx.takePending() == null);
}
