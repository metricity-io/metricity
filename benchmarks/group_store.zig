const std = @import("std");
const metricity = @import("metricity");

const runtime = metricity.sql.runtime;
const parser = metricity.sql.parser;
const source_mod = metricity.source;
const event_mod = source_mod.event;

const ScenarioResult = struct {
    count: usize,
    insert_ns: u64,
    lookup_ns: u64,

    fn insertNsPerEvent(self: ScenarioResult) f128 {
        if (self.count == 0) return 0;
        return @as(f128, @floatFromInt(self.insert_ns)) / @as(f128, @floatFromInt(self.count));
    }

    fn lookupNsPerEvent(self: ScenarioResult) f128 {
        if (self.count == 0) return 0;
        return @as(f128, @floatFromInt(self.lookup_ns)) / @as(f128, @floatFromInt(self.count));
    }

    fn insertThroughput(self: ScenarioResult) f128 {
        if (self.insert_ns == 0) return 0;
        return @as(f128, @floatFromInt(self.count)) * @as(f128, @floatFromInt(std.time.ns_per_s)) /
            @as(f128, @floatFromInt(self.insert_ns));
    }

    fn lookupThroughput(self: ScenarioResult) f128 {
        if (self.lookup_ns == 0) return 0;
        return @as(f128, @floatFromInt(self.count)) * @as(f128, @floatFromInt(std.time.ns_per_s)) /
            @as(f128, @floatFromInt(self.lookup_ns));
    }
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    var counts = try parseCounts(allocator, args);
    defer allocator.free(counts);

    if (counts.len == 0) {
        const defaults = [_]usize{ 1_000, 10_000, 100_000, 250_000 };
        counts = try allocator.alloc(usize, defaults.len);
        @memcpy(counts, &defaults);
    }

    std.debug.print("scenario,count,phase,elapsed_ns,ns_per_event,events_per_sec\n", .{});

    for (counts) |count| {
        const result = try runScenario(allocator, count);
        const insert_ns_per_event = result.insertNsPerEvent();
        const lookup_ns_per_event = result.lookupNsPerEvent();
        const insert_throughput = result.insertThroughput();
        const lookup_throughput = result.lookupThroughput();

        std.debug.print(
            "group_store,{d},insert,{d},{d:.2},{d:.2}\n",
            .{ count, result.insert_ns, insert_ns_per_event, insert_throughput },
        );
        std.debug.print(
            "group_store,{d},lookup,{d},{d:.2},{d:.2}\n",
            .{ count, result.lookup_ns, lookup_ns_per_event, lookup_throughput },
        );
    }
}

fn parseCounts(allocator: std.mem.Allocator, args: [][:0]u8) ![]usize {
    if (args.len <= 1) return allocator.alloc(usize, 0);
    const raw = args[1..];
    var list = std.ArrayListUnmanaged(usize){};
    errdefer list.deinit(allocator);

    for (raw) |arg| {
        const trimmed = std.mem.trim(u8, std.mem.sliceTo(arg, 0), " ");
        if (trimmed.len == 0) continue;
        const value = std.fmt.parseInt(usize, trimmed, 10) catch |err| switch (err) {
            error.InvalidCharacter => continue,
            else => return err,
        };
        try list.append(allocator, value);
    }

    const owned = try list.toOwnedSlice(allocator);
    list.deinit(allocator);
    return owned;
}

fn runScenario(allocator: std.mem.Allocator, count: usize) !ScenarioResult {
    var arena_inst = std.heap.ArenaAllocator.init(allocator);
    defer arena_inst.deinit();

    const query = "SELECT message, COUNT(*) AS total FROM logs GROUP BY message";
    const stmt = try parser.parseSelect(arena_inst.allocator(), query);

    var program = try runtime.compile(allocator, stmt, .{ .eviction = .{
        .ttl_ns = null,
        .max_groups = if (count == 0) 0 else count,
        .sweep_interval_ns = 0,
    } });
    defer program.deinit();

    var clock: u64 = 1;

    const warmup_base = count / 10;
    const warmup_count = if (warmup_base < 100_000) warmup_base else 100_000;
    if (warmup_count > 0) {
        try processEvents(&program, allocator, warmup_count, &clock);
        program.reset();
        clock = 1;
    }

    var insert_timer = try std.time.Timer.start();
    try processEvents(&program, allocator, count, &clock);
    const insert_ns = insert_timer.read();

    var lookup_timer = try std.time.Timer.start();
    try processEvents(&program, allocator, count, &clock);
    const lookup_ns = lookup_timer.read();

    return ScenarioResult{
        .count = count,
        .insert_ns = insert_ns,
        .lookup_ns = lookup_ns,
    };
}

fn processEvents(program: *runtime.Program, allocator: std.mem.Allocator, count: usize, clock: *u64) !void {
    var i: usize = 0;
    while (i < count) : (i += 1) {
        const message = try std.fmt.allocPrint(allocator, "group_{d}", .{i});
        defer allocator.free(message);

        var event = try createEvent(allocator, message);
        defer freeEvent(allocator, event);

        if (try program.execute(allocator, &event, nextTimestamp(clock))) |row| {
            row.deinit();
        }
    }
}

fn createEvent(allocator: std.mem.Allocator, message: []const u8) !event_mod.Event {
    const fields = try allocator.alloc(event_mod.Field, 1);
    fields[0] = .{ .name = "syslog_severity", .value = .{ .integer = 5 } };

    return event_mod.Event{
        .metadata = .{ .source_id = "benchmark" },
        .payload = .{ .log = .{ .message = message, .fields = fields } },
    };
}

fn freeEvent(allocator: std.mem.Allocator, event: event_mod.Event) void {
    switch (event.payload) {
        .log => |log_event| allocator.free(log_event.fields),
    }
}

fn nextTimestamp(counter: *u64) u64 {
    const value = counter.*;
    counter.* += 1;
    return value;
}
