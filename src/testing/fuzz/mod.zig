//! Helpers for fuzz testing Metricity components.

const std = @import("std");

pub const common = @import("common.zig");
pub const config = @import("config.zig");
pub const sql = @import("sql.zig");
pub const syslog_framer = @import("syslog_framer.zig");
pub const syslog_parser = @import("syslog_parser.zig");

pub const Config = struct {
    seed: ?u64 = null,
    max_iterations: usize = 512,
    max_input_len: usize = 2048,
};

pub const Runner = struct {
    config: Config,
    prng: std.Random.DefaultPrng,

    pub fn init(cfg: Config) Runner {
        const seed = cfg.seed orelse @as(u64, 0xC0FFEE);
        return .{
            .config = cfg,
            .prng = std.Random.DefaultPrng.init(seed),
        };
    }

    pub fn run(
        self: *Runner,
        context: anytype,
        comptime testOne: fn (context: @TypeOf(context), input: []const u8) anyerror!void,
        options: std.testing.FuzzInputOptions,
    ) anyerror!void {
        const allocator = inferAllocator(context);

        for (options.corpus) |entry| {
            try testOne(context, entry);
        }

        var iteration: usize = 0;
        while (iteration < self.config.max_iterations) : (iteration += 1) {
            if (try maybeGenerateInput(self, context, allocator)) |generated| {
                defer allocator.free(generated);
                try testOne(context, generated);
                continue;
            }

            const buffer = try common.randomBytes(&self.prng, allocator, self.config.max_input_len);
            defer allocator.free(buffer);
            try testOne(context, buffer);
        }
    }
};

pub fn run(
    context: anytype,
    comptime testOne: fn (context: @TypeOf(context), input: []const u8) anyerror!void,
    options: std.testing.FuzzInputOptions,
    base_config: Config,
) anyerror!void {
    const resolved = inferConfig(context, base_config);
    var runner = Runner.init(resolved);
    try runner.run(context, testOne, options);
}

fn inferAllocator(context: anytype) std.mem.Allocator {
    const T = @TypeOf(context);
    return switch (@typeInfo(T)) {
        .pointer => inferAllocator(context.*),
        else => blk: {
            if (@hasField(T, "allocator")) break :blk @field(context, "allocator");
            break :blk std.testing.allocator;
        },
    };
}

fn inferConfig(context: anytype, fallback: Config) Config {
    const T = @TypeOf(context);
    return switch (@typeInfo(T)) {
        .pointer => inferConfig(context.*, fallback),
        else => blk: {
            if (@hasField(T, "fuzz_config")) {
                const value = @field(context, "fuzz_config");
                if (@TypeOf(value) == Config) break :blk value;
            }
            break :blk fallback;
        },
    };
}

fn maybeGenerateInput(
    runner: *Runner,
    context: anytype,
    allocator: std.mem.Allocator,
) !?[]u8 {
    const T = @TypeOf(context);
    return switch (@typeInfo(T)) {
        .pointer => |ptr| blk: {
            const Child = ptr.child;
            if (@hasDecl(Child, "generateInput")) {
                break :blk try context.generateInput(&runner.prng, allocator);
            }
            break :blk null;
        },
        else => null,
    };
}
