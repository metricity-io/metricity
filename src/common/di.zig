//! Generic dependency injection helpers used to wire runtime services.

const std = @import("std");

pub fn Container(comptime ServiceSet: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        services: ServiceSet,

        pub fn init(allocator: std.mem.Allocator, services: ServiceSet) Self {
            return .{ .allocator = allocator, .services = services };
        }

        pub fn withOverrides(self: *const Self, overrides: ServiceSet) Self {
            return .{ .allocator = self.allocator, .services = overrides };
        }

        pub fn withPartial(self: *const Self, comptime Override: type, override: Override) Self {
            var merged = self.services;
            inline for (std.meta.fields(Override)) |field| {
                if (!@hasField(@TypeOf(merged), field.name)) {
                    @compileError("Unknown service: " ++ field.name);
                }
                @field(merged, field.name) = @field(override, field.name);
            }
            return .{ .allocator = self.allocator, .services = merged };
        }

        pub fn view(self: *const Self) *const ServiceSet {
            return &self.services;
        }

        pub fn deinit(self: *Self) void {
            inline for (std.meta.fields(@TypeOf(self.services))) |field| {
                const FieldType = @TypeOf(@field(self.services, field.name));
                if (comptime hasAllocatorDeinit(FieldType)) {
                    const field_ptr = &@field(self.services, field.name);
                    field_ptr.deinit(self.allocator);
                }
            }
        }

        inline fn hasAllocatorDeinit(comptime FieldType: type) bool {
            switch (@typeInfo(FieldType)) {
                .@"struct" => {},
                else => return false,
            }

            if (!@hasDecl(FieldType, "deinit")) return false;

            const fn_type = @TypeOf(@field(FieldType, "deinit"));
            const info = @typeInfo(fn_type);
            if (info != .Fn) return false;

            const fn_info = info.Fn;
            if (fn_info.params.len != 2) return false;

            const param0 = fn_info.params[0].type orelse return false;
            if (param0 != *FieldType) return false;

            const param1 = fn_info.params[1].type orelse return false;
            if (param1 != std.mem.Allocator) return false;

            const ret = fn_info.return_type orelse return false;
            return ret == void;
        }
    };
}

pub fn Lazy(comptime T: type) type {
    return struct {
        const Self = @This();

        pub const InitFn = *const fn (std.mem.Allocator) anyerror!T;
        pub const DeinitFn = *const fn (std.mem.Allocator, *T) void;

        init_fn: InitFn,
        deinit_fn: ?DeinitFn = null,

        ptr: ?*T = null,
        lock: std.Thread.Mutex = .{},

        pub fn init(init_fn: InitFn, deinit_fn: ?DeinitFn) Self {
            return .{
                .init_fn = init_fn,
                .deinit_fn = deinit_fn,
                .ptr = null,
                .lock = .{},
            };
        }

        pub fn get(self: *Self, allocator: std.mem.Allocator) !*T {
            if (@atomicLoad(?*T, &self.ptr, .Acquire)) |existing| {
                return existing;
            }

            self.lock.lock();
            defer self.lock.unlock();

            if (@atomicLoad(?*T, &self.ptr, .Acquire)) |raced| {
                return raced;
            }

            const storage = try allocator.create(T);
            errdefer allocator.destroy(storage);

            storage.* = try self.init_fn(allocator);

            @atomicStore(?*T, &self.ptr, storage, .Release);
            return storage;
        }

        pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
            self.lock.lock();
            defer self.lock.unlock();

            const existing = @atomicLoad(?*T, &self.ptr, .Acquire) orelse return;
            @atomicStore(?*T, &self.ptr, null, .Release);

            if (self.deinit_fn) |destroy| {
                destroy(allocator, existing);
            } else {
                allocator.destroy(existing);
            }
        }
    };
}

test "Lazy initializes once and recreates after deinit" {
    const allocator = std.testing.allocator;

    const Fixture = struct {
        var init_calls: usize = 0;

        fn make(_: std.mem.Allocator) anyerror!usize {
            init_calls += 1;
            return init_calls;
        }
    };

    var lazy = Lazy(usize).init(Fixture.make, null);

    const first = try lazy.get(allocator);
    try std.testing.expectEqual(@as(usize, 1), first.*);

    const second = try lazy.get(allocator);
    try std.testing.expectEqual(first, second);
    try std.testing.expectEqual(@as(usize, 1), Fixture.init_calls);

    lazy.deinit(allocator);
    try std.testing.expectEqual(
        @as(?*usize, null),
        @atomicLoad(?*usize, &lazy.ptr, .Acquire),
    );

    const third = try lazy.get(allocator);
    try std.testing.expectEqual(@as(usize, 2), third.*);
    try std.testing.expectEqual(@as(usize, 2), Fixture.init_calls);

    lazy.deinit(allocator);
}

test "Lazy uses custom deinit callback" {
    const allocator = std.testing.allocator;

    const Tracker = struct {
        var destroy_calls: usize = 0;

        fn make(_: std.mem.Allocator) anyerror!usize {
            return 42;
        }

        fn destroy(allocator_: std.mem.Allocator, ptr: *usize) void {
            destroy_calls += 1;
            allocator_.destroy(ptr);
        }
    };

    var lazy = Lazy(usize).init(Tracker.make, Tracker.destroy);

    _ = try lazy.get(allocator);
    lazy.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), Tracker.destroy_calls);

    lazy.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), Tracker.destroy_calls);
}

test "Container deinit runs lazy services" {
    const allocator = std.testing.allocator;

    const Tracker = struct {
        var destroy_calls: usize = 0;

        fn make(_: std.mem.Allocator) anyerror!usize {
            return 7;
        }

        fn destroy(allocator_: std.mem.Allocator, ptr: *usize) void {
            destroy_calls += 1;
            allocator_.destroy(ptr);
        }
    };

    const Services = struct {
        lazy: Lazy(usize),
        passthrough: *const fn () void,
    };

    const Passthrough = struct {
        fn call() void {}
    };

    Tracker.destroy_calls = 0;

    var container = Container(Services).init(allocator, .{
        .lazy = Lazy(usize).init(Tracker.make, Tracker.destroy),
        .passthrough = Passthrough.call,
    });

    const services_view = container.view();
    const lazy_ptr = @constCast(&services_view.lazy);
    _ = try lazy_ptr.get(allocator);

    container.deinit();
    try std.testing.expectEqual(@as(usize, 1), Tracker.destroy_calls);

    container.deinit();
    try std.testing.expectEqual(@as(usize, 1), Tracker.destroy_calls);
}
