const std = @import("std");

/// Shared arena pool used by sources to provide deterministic event lifetimes.
pub const EventArenaPool = struct {
    pub const Error = error{OutOfMemory};

    pub const Arena = struct {
        arena: std.heap.ArenaAllocator,
        next: ?*Arena = null,

        pub fn allocator(self: *Arena) std.mem.Allocator {
            return self.arena.allocator();
        }
    };

    allocator: std.mem.Allocator,
    free_list: ?*Arena = null,

    pub fn init(allocator: std.mem.Allocator) EventArenaPool {
        return .{ .allocator = allocator, .free_list = null };
    }

    pub fn deinit(self: *EventArenaPool) void {
        var cursor = self.free_list;
        while (cursor) |arena| {
            const next = arena.next;
            arena.arena.deinit();
            self.allocator.destroy(arena);
            cursor = next;
        }
        self.free_list = null;
    }

    pub fn acquire(self: *EventArenaPool) Error!*Arena {
        if (self.free_list) |arena| {
            self.free_list = arena.next;
            arena.next = null;
            return arena;
        }

        const arena = self.allocator.create(Arena) catch return Error.OutOfMemory;
        arena.* = .{
            .arena = std.heap.ArenaAllocator.init(self.allocator),
            .next = null,
        };
        return arena;
    }

    pub fn release(self: *EventArenaPool, arena: *Arena) void {
        _ = arena.arena.reset(.retain_capacity);
        arena.next = self.free_list;
        self.free_list = arena;
    }

    pub fn freeCount(self: *const EventArenaPool) usize {
        var cursor = self.free_list;
        var count: usize = 0;
        while (cursor) |arena| {
            count += 1;
            cursor = arena.next;
        }
        return count;
    }
};
