const xev = @import("xev");
const loop = @import("loop.zig");

/// Shared runtime wrapping a single libxev event loop.
pub const IoRuntime = struct {
    loop: loop.EventLoop,

    pub fn init(options: xev.Options) !IoRuntime {
        return .{ .loop = try loop.EventLoop.init(options) };
    }

    pub fn initDefault() !IoRuntime {
        return .{ .loop = try loop.EventLoop.initDefault() };
    }

    pub fn deinit(self: *IoRuntime) void {
        self.loop.deinit();
    }

    pub fn loopHandle(self: *IoRuntime) *xev.Loop {
        return self.loop.handle();
    }

    pub fn tick(self: *IoRuntime) !void {
        try self.loop.tick();
    }
};
