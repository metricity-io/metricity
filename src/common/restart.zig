const std = @import("std");

pub const BackoffJitter = enum { full, plusminus, decorrelated };

pub const BackoffPolicy = struct {
    min_ns: u64,
    max_ns: u64,
    multiplier_num: u32,
    multiplier_den: u32,
    jitter: BackoffJitter,
    jitter_ratio_ppm: u32 = 0,
};

pub const BackoffState = struct {
    current_ns: u64 = 0,
    prev_ns: u64 = 0,
    resume_at_ns: u128 = 0,
    attempts: u32 = 0,
};

pub const FailureBudgetPolicy = struct {
    window_ns: u64,
    limit: u8,
    hold_ns: u64,
};

const max_tracked_failures: u8 = 16;
const failure_ring_capacity: usize = max_tracked_failures;

pub const FailureBudgetState = struct {
    entries: [max_tracked_failures]u128 = undefined,
    head: u8 = 0,
    count: u8 = 0,
    hold_until_ns: u128 = 0,
};

pub const RestartPolicy = struct {
    backoff: BackoffPolicy,
    budget: FailureBudgetPolicy,
};

pub const RandomSource = struct {
    context: ?*anyopaque = null,
    next_fn: ?*const fn (context: ?*anyopaque) u64 = null,

    pub fn next(self: RandomSource) u64 {
        if (self.next_fn) |callback| {
            return callback(self.context);
        }
        return 0;
    }
};

pub const RestartDecision = struct {
    pending: bool,
    resume_at: u128,
};

pub fn RestartState(
    comptime FailureKindT: type,
    comptime FailurePermanenceT: type,
) type {
    return struct {
        const Self = @This();

        pub const FailureKind = FailureKindT;
        pub const FailurePermanence = FailurePermanenceT;

        backoff: BackoffState = .{},
        budget: FailureBudgetState = .{},
        pending_restart: bool = false,
        last_failure_kind: ?FailureKindT = null,
        last_failure_permanence: ?FailurePermanenceT = null,
        random: RandomSource = .{},

        pub fn init(random: RandomSource) Self {
            return .{ .random = random };
        }

        pub fn setRandom(self: *Self, random: RandomSource) void {
            self.random = random;
        }
    };
}

pub fn readyAt(state: anytype) ?u128 {
    if (state.*.pending_restart) {
        return state.*.backoff.resume_at_ns;
    }
    return null;
}

pub fn noteFailure(
    state: anytype,
    kind: @TypeOf(state.*).FailureKind,
    permanence: @TypeOf(state.*).FailurePermanence,
) void {
    state.*.last_failure_kind = kind;
    state.*.last_failure_permanence = permanence;
}

pub fn acceptFailure(
    state: anytype,
    policy: RestartPolicy,
    kind: @TypeOf(state.*).FailureKind,
    permanence: @TypeOf(state.*).FailurePermanence,
    now: u128,
) RestartDecision {
    noteFailure(state, kind, permanence);
    _ = recordFailureEvent(&state.*.budget, policy.budget, now);
    return nextBackoff(state, policy, now);
}

pub fn nextBackoff(
    state: anytype,
    policy: RestartPolicy,
    now: u128,
) RestartDecision {
    if (isUnderQuarantine(state, now)) {
        state.*.backoff.current_ns = policy.backoff.max_ns;
        state.*.backoff.prev_ns = policy.backoff.max_ns;
        state.*.backoff.resume_at_ns = state.*.budget.hold_until_ns;
        state.*.pending_restart = true;
        return .{
            .pending = true,
            .resume_at = state.*.backoff.resume_at_ns,
        };
    }

    _ = nextBackoffInterval(policy.backoff, &state.*.backoff);
    const jitter_ns = jitterDelay(policy.backoff, &state.*.backoff, state.*.random.next());
    var resume_at = now + @as(u128, jitter_ns);
    if (isUnderQuarantine(state, resume_at)) {
        if (resume_at < state.*.budget.hold_until_ns) {
            resume_at = state.*.budget.hold_until_ns;
        }
        state.*.backoff.current_ns = policy.backoff.max_ns;
        state.*.backoff.prev_ns = policy.backoff.max_ns;
    }
    state.*.backoff.resume_at_ns = resume_at;
    state.*.pending_restart = true;
    return .{
        .pending = true,
        .resume_at = resume_at,
    };
}

pub fn recordSuccess(state: anytype) void {
    resetBackoff(&state.*.backoff);
    clearFailureBudget(&state.*.budget);
    state.*.pending_restart = false;
    state.*.last_failure_kind = null;
    state.*.last_failure_permanence = null;
}

pub fn isUnderQuarantine(state: anytype, timestamp: u128) bool {
    const hold_until = state.*.budget.hold_until_ns;
    return hold_until != 0 and timestamp < hold_until;
}

fn resetBackoff(state: *BackoffState) void {
    state.* = .{};
}

fn mulDiv(value: u64, num: u32, den: u32) u64 {
    std.debug.assert(den != 0);
    const numerator = @as(u128, value) * num;
    const quotient = numerator / den;
    if (quotient > std.math.maxInt(u64)) return std.math.maxInt(u64);
    return @as(u64, @intCast(quotient));
}

fn nextBackoffInterval(policy: BackoffPolicy, state: *BackoffState) u64 {
    if (state.current_ns == 0) {
        state.current_ns = policy.min_ns;
        state.prev_ns = state.current_ns;
        state.attempts = 1;
        return state.current_ns;
    }

    var grown = mulDiv(state.current_ns, policy.multiplier_num, policy.multiplier_den);
    if (grown < policy.min_ns) grown = policy.min_ns;
    if (grown > policy.max_ns) grown = policy.max_ns;
    state.prev_ns = state.current_ns;
    state.current_ns = grown;
    if (state.attempts < std.math.maxInt(u32)) state.attempts += 1;
    return state.current_ns;
}

fn jitterDelay(policy: BackoffPolicy, state: *BackoffState, random_value: u64) u64 {
    const base = state.current_ns;
    if (base == 0) return 0;

    return switch (policy.jitter) {
        .full => {
            const range = @as(u128, base) + 1;
            return @as(u64, @intCast(@as(u128, random_value) % range));
        },
        .plusminus => {
            if (policy.jitter_ratio_ppm == 0) return base;
            const ratio = policy.jitter_ratio_ppm;
            const span = (@as(u128, base) * ratio) / 1_000_000;
            if (span == 0) return base;
            const total = span * 2 + 1;
            const sample = @as(u128, random_value) % total;
            const offset = @as(i128, @intCast(sample)) - @as(i128, @intCast(span));
            const adjusted = @as(i128, @intCast(base)) + offset;
            if (adjusted <= 0) return 0;
            const positive = @as(u128, @intCast(adjusted));
            if (positive > policy.max_ns) return policy.max_ns;
            return @as(u64, @intCast(positive));
        },
        .decorrelated => {
            const prev = if (state.prev_ns == 0) policy.min_ns else state.prev_ns;
            const candidate = mulDiv(prev, policy.multiplier_num, policy.multiplier_den);
            const hi = if (candidate > policy.max_ns) policy.max_ns else candidate;
            const lo = if (hi < policy.min_ns) hi else policy.min_ns;
            const span = @as(u128, hi - lo) + 1;
            const sample = @as(u128, random_value) % span;
            return @as(u64, @intCast(@as(u128, lo) + sample));
        },
    };
}

fn pruneFailureEntries(state: *FailureBudgetState, policy: FailureBudgetPolicy, now: u128) void {
    if (state.count == 0) return;
    const window = @as(u128, policy.window_ns);
    while (state.count > 0) {
        const oldest_index_u8: u8 = if (state.head >= state.count)
            state.head - state.count
        else
            state.head + max_tracked_failures - state.count;
        const oldest_index = @as(usize, @intCast(oldest_index_u8));
        const ts = state.entries[oldest_index];
        const delta = if (now >= ts) now - ts else ts - now;
        if (delta > window) {
            state.count -= 1;
            continue;
        }
        break;
    }
}

fn recordFailureEvent(state: *FailureBudgetState, policy: FailureBudgetPolicy, now: u128) bool {
    pruneFailureEntries(state, policy, now);

    state.entries[@as(usize, @intCast(state.head))] = now;
    const next_head = (@as(usize, @intCast(state.head)) + 1) % failure_ring_capacity;
    state.head = @as(u8, @intCast(next_head));
    if (state.count < max_tracked_failures) {
        state.count += 1;
    }

    if (state.count >= policy.limit and policy.limit != 0) {
        state.hold_until_ns = now + @as(u128, policy.hold_ns);
        return true;
    }
    return false;
}

fn clearFailureBudget(state: *FailureBudgetState) void {
    state.* = .{};
}

const testing = std.testing;

const TestKind = enum { one, two };
const TestPermanence = enum { transient, permanent };

const RandomState = struct {
    value: u64,
};

fn randomFromState(context: ?*anyopaque) u64 {
    std.debug.assert(context != null);
    const aligned: *align(@alignOf(RandomState)) anyopaque = @alignCast(context.?);
    const state: *RandomState = @ptrCast(aligned);
    return state.value;
}

test "acceptFailure schedules backoff with jitter and tracks last failure" {
    var random_state = RandomState{ .value = 5 };
    var state = RestartState(TestKind, TestPermanence).init(.{
        .context = &random_state,
        .next_fn = randomFromState,
    });
    const policy = RestartPolicy{
        .backoff = .{
            .min_ns = 10,
            .max_ns = 100,
            .multiplier_num = 2,
            .multiplier_den = 1,
            .jitter = .full,
        },
        .budget = .{
            .window_ns = 1_000,
            .limit = 3,
            .hold_ns = 5_000,
        },
    };

    const now: u128 = 100;
    const decision = acceptFailure(&state, policy, .one, .transient, now);
    try testing.expect(decision.pending);
    try testing.expectEqual(now + 5, decision.resume_at);
    try testing.expectEqual(TestKind.one, state.last_failure_kind.?);
    try testing.expectEqual(TestPermanence.transient, state.last_failure_permanence.?);
    try testing.expectEqual(@as(u64, 10), state.backoff.current_ns);
    try testing.expectEqual(now + 5, readyAt(&state).?);

    random_state.value = 7;
    const next_now = now + 50;
    const second = nextBackoff(&state, policy, next_now);
    try testing.expect(second.pending);
    try testing.expectEqual(next_now + 7, second.resume_at);
    try testing.expectEqual(@as(u64, 20), state.backoff.current_ns);

    recordSuccess(&state);
    try testing.expectEqual(@as(?TestKind, null), state.last_failure_kind);
    try testing.expectEqual(@as(?TestPermanence, null), state.last_failure_permanence);
    try testing.expectEqual(false, state.pending_restart);
    try testing.expect(readyAt(&state) == null);
}

test "acceptFailure respects failure budget quarantine" {
    var random_state = RandomState{ .value = 3 };
    var state = RestartState(TestKind, TestPermanence).init(.{
        .context = &random_state,
        .next_fn = randomFromState,
    });

    const policy = RestartPolicy{
        .backoff = .{
            .min_ns = 10,
            .max_ns = 100,
            .multiplier_num = 2,
            .multiplier_den = 1,
            .jitter = .full,
        },
        .budget = .{
            .window_ns = 10_000,
            .limit = 1,
            .hold_ns = 2_000,
        },
    };

    const now: u128 = 0;
    const decision = acceptFailure(&state, policy, .two, .transient, now);
    try testing.expect(decision.pending);
    try testing.expectEqual(@as(u128, policy.budget.hold_ns), decision.resume_at);
    try testing.expect(isUnderQuarantine(&state, now));
    try testing.expectEqual(@as(u64, policy.backoff.max_ns), state.backoff.current_ns);

    const later: u128 = policy.budget.hold_ns - 1;
    try testing.expect(isUnderQuarantine(&state, later));
    try testing.expectEqual(policy.budget.hold_ns, readyAt(&state).?);

    const cleared_time = policy.budget.hold_ns;
    recordSuccess(&state);
    try testing.expect(!isUnderQuarantine(&state, cleared_time));
}
