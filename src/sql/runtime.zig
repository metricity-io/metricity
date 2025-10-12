const std = @import("std");
const ast = @import("ast.zig");
const source_mod = @import("source");
const event_mod = source_mod.event;

const ascii = std.ascii;

const IdentifierBinding = struct {
    canonical: []const u8,
    value: []const u8,
    quoted: bool,
};

const TableBinding = struct {
    table: IdentifierBinding,
    alias: ?IdentifierBinding = null,
};

/// Error set representing SQL compilation or evaluation failures triggered by
/// user-provided statements.
pub const Error = std.mem.Allocator.Error || error{
    UnsupportedFeature,
    UnknownColumn,
    TypeMismatch,
    UnsupportedFunction,
    DivideByZero,
    ArithmeticOverflow,
};

/// Compiled representation of a SQL `SELECT` statement that can be executed
/// against pipeline events.
pub const Program = struct {
    allocator: std.mem.Allocator,
    projection: []const Projection,
    selection: ?*const ast.Expression,
    table_binding: ?TableBinding,

    /// Releases memory allocated for the projection metadata.
    pub fn deinit(self: Program) void {
        self.allocator.free(self.projection);
    }

    /// Evaluates the program against a single event, returning an optional row.
    /// When the `WHERE` clause filters out the event, `null` is returned.
    pub fn execute(self: Program, allocator: std.mem.Allocator, event: *const event_mod.Event) Error!?Row {
        const binding = self.table_binding;
        if (self.selection) |expr| {
            const predicate = try evaluateBoolean(expr, event, binding);
            if (!predicate) return null;
        }

        const total_columns = projectedColumnCount(self.projection, event);
        var values = try allocator.alloc(ValueEntry, total_columns);
        var idx: usize = 0;
        errdefer allocator.free(values);

        for (self.projection) |proj| {
            switch (proj) {
                .expression => |expr_proj| {
                    const value = try evaluateExpression(expr_proj.expr, event, binding);
                    values[idx] = .{ .name = expr_proj.label, .value = value };
                    idx += 1;
                },
                .star => |star_proj| {
                    const emitted = try emitStarColumns(values[idx..], event, binding, star_proj);
                    idx += emitted;
                },
            }
        }

        std.debug.assert(idx == total_columns);
        return Row{ .allocator = allocator, .values = values };
    }
};

/// Lightweight wrapper describing a compiled projection element.
pub const Projection = union(enum) {
    expression: Expression,
    star: Star,
};

pub const Expression = struct {
    label: []const u8,
    expr: *const ast.Expression,
};

pub const Star = struct {
    qualifier: ?IdentifierBinding,
};

/// Result row consumed by sink implementations.
pub const Row = struct {
    allocator: std.mem.Allocator,
    values: []ValueEntry,

    /// Releases ownership of value entries.
    pub fn deinit(self: Row) void {
        self.allocator.free(self.values);
    }
};

/// Named value produced by SQL execution.
pub const ValueEntry = struct {
    name: []const u8,
    value: event_mod.Value,
};

pub fn compile(allocator: std.mem.Allocator, stmt: *const ast.SelectStatement) Error!Program {
    if (stmt.distinct) return Error.UnsupportedFeature;
    if (stmt.group_by.len != 0) return Error.UnsupportedFeature;
    if (stmt.order_by.len != 0) return Error.UnsupportedFeature;
    if (stmt.having) |_| return Error.UnsupportedFeature;
    if (stmt.from.len > 1) return Error.UnsupportedFeature;

    var projections = std.ArrayListUnmanaged(Projection){};
    defer projections.deinit(allocator);

    for (stmt.projection) |item| {
        switch (item.kind) {
            .star => |star| {
                const qualifier = if (star.qualifier) |ident| identifierBinding(ident) else null;
                try projections.append(allocator, .{ .star = .{ .qualifier = qualifier } });
            },
            .expression => |expr| {
                const label = if (item.alias) |alias_ident| alias_ident.text() else deriveLabel(expr);
                try projections.append(allocator, .{ .expression = .{ .label = label, .expr = expr } });
            },
        }
    }

    const projection_slice = try projections.toOwnedSlice(allocator);
    const table_binding = try determineTableBinding(stmt);

    return Program{
        .allocator = allocator,
        .projection = projection_slice,
        .selection = stmt.selection,
        .table_binding = table_binding,
    };
}

fn deriveLabel(expr: *const ast.Expression) []const u8 {
    return switch (expr.*) {
        .column => |col| col.name.text(),
        else => "<expr>",
    };
}

fn projectedColumnCount(projection: []const Projection, event: *const event_mod.Event) usize {
    var total: usize = 0;
    for (projection) |proj| {
        switch (proj) {
            .expression => total += 1,
            .star => |star_proj| total += starColumnCount(event, star_proj),
        }
    }
    return total;
}

fn starColumnCount(event: *const event_mod.Event, star: Star) usize {
    _ = star;
    return switch (event.payload) {
        .log => |log_event| 1 + log_event.fields.len,
    };
}

fn emitStarColumns(dest: []ValueEntry, event: *const event_mod.Event, binding: ?TableBinding, star: Star) Error!usize {
    if (star.qualifier) |qualifier| {
        if (!starQualifierAllowed(binding, qualifier)) {
            return Error.UnknownColumn;
        }
    }

    return switch (event.payload) {
        .log => |log_event| {
            std.debug.assert(dest.len >= 1 + log_event.fields.len);
            var filled: usize = 0;
            dest[filled] = .{ .name = "message", .value = .{ .string = log_event.message } };
            filled += 1;
            for (log_event.fields) |field| {
                dest[filled] = .{ .name = field.name, .value = field.value };
                filled += 1;
            }
            return filled;
        },
    };
}

fn evaluateBoolean(expr: *const ast.Expression, event: *const event_mod.Event, binding: ?TableBinding) Error!bool {
    const value = try evaluateExpression(expr, event, binding);
    return switch (value) {
        .boolean => |b| b,
        else => Error.TypeMismatch,
    };
}

fn evaluateExpression(expr: *const ast.Expression, event: *const event_mod.Event, binding: ?TableBinding) Error!event_mod.Value {
    return switch (expr.*) {
        .literal => |lit| valueFromLiteral(lit),
        .column => |col| resolveColumn(event, col, binding),
        .unary => |unary| try evalUnary(unary, event, binding),
        .binary => |binary| try evalBinary(binary, event, binding),
        .function_call => |_| Error.UnsupportedFunction,
        .star => Error.UnsupportedFeature,
    };
}

fn valueFromLiteral(lit: ast.Literal) Error!event_mod.Value {
    return switch (lit.value) {
        .integer => |value| event_mod.Value{ .integer = value },
        .float => |text| {
            const parsed = std.fmt.parseFloat(f64, text) catch return Error.TypeMismatch;
            return event_mod.Value{ .float = parsed };
        },
        .string => |text| event_mod.Value{ .string = text },
        .boolean => |b| event_mod.Value{ .boolean = b },
        .null => event_mod.Value{ .null = {} },
    };
}

fn resolveColumn(event: *const event_mod.Event, col: ast.ColumnRef, binding: ?TableBinding) Error!event_mod.Value {
    return switch (event.payload) {
        .log => |log_event| resolveLogColumn(event, log_event, col, binding),
    };
}

fn resolveLogColumn(ev: *const event_mod.Event, log_event: event_mod.LogEvent, col: ast.ColumnRef, binding: ?TableBinding) Error!event_mod.Value {
    if (col.table) |qualifier| {
        if (!qualifierAllowed(binding, qualifier)) {
            return Error.UnknownColumn;
        }
    }

    const identifier = col.name;
    const canonical = identifier.canonical;

    if (!identifier.quoted and std.mem.eql(u8, canonical, "message")) {
        return event_mod.Value{ .string = log_event.message };
    }

    if (!identifier.quoted and std.mem.eql(u8, canonical, "source_id")) {
        if (ev.metadata.source_id) |source_id| {
            return event_mod.Value{ .string = source_id };
        }
        return event_mod.Value{ .null = {} };
    }

    for (log_event.fields) |field| {
        if (identifier.quoted) {
            if (std.mem.eql(u8, field.name, identifier.value)) {
                return field.value;
            }
        } else {
            if (ascii.eqlIgnoreCase(field.name, canonical)) {
                return field.value;
            }
        }
    }

    return event_mod.Value{ .null = {} };
}

fn evalUnary(unary: ast.UnaryExpr, event: *const event_mod.Event, binding: ?TableBinding) Error!event_mod.Value {
    const operand = try evaluateExpression(unary.operand, event, binding);
    return switch (unary.op) {
        .plus => operand,
        .minus => switch (operand) {
            .integer => |value| event_mod.Value{ .integer = -value },
            .float => |value| event_mod.Value{ .float = -value },
            else => Error.TypeMismatch,
        },
        .not => switch (operand) {
            .boolean => |value| event_mod.Value{ .boolean = !value },
            else => Error.TypeMismatch,
        },
    };
}

fn evalBinary(binary: ast.BinaryExpr, event: *const event_mod.Event, binding: ?TableBinding) Error!event_mod.Value {
    const left = try evaluateExpression(binary.left, event, binding);
    const right = try evaluateExpression(binary.right, event, binding);

    return switch (binary.op) {
        .add => try addValues(left, right),
        .subtract => try subtractValues(left, right),
        .multiply => try multiplyValues(left, right),
        .divide => try divideValues(left, right),
        .equal => event_mod.Value{ .boolean = compareEqual(left, right) },
        .not_equal => event_mod.Value{ .boolean = !compareEqual(left, right) },
        .less => event_mod.Value{ .boolean = try compareOrder(.less, left, right) },
        .less_equal => event_mod.Value{ .boolean = try compareOrder(.less_equal, left, right) },
        .greater => event_mod.Value{ .boolean = try compareOrder(.greater, left, right) },
        .greater_equal => event_mod.Value{ .boolean = try compareOrder(.greater_equal, left, right) },
        .logical_and => event_mod.Value{ .boolean = try logicalAnd(left, right) },
        .logical_or => event_mod.Value{ .boolean = try logicalOr(left, right) },
    };
}

fn addValues(left: event_mod.Value, right: event_mod.Value) Error!event_mod.Value {
    if (left == .integer and right == .integer) {
        const ov = @addWithOverflow(left.integer, right.integer);
        if (ov[1] != 0) return Error.ArithmeticOverflow;
        return event_mod.Value{ .integer = ov[0] };
    }

    const lf = try toFloat(left);
    const rf = try toFloat(right);
    return event_mod.Value{ .float = lf + rf };
}

fn subtractValues(left: event_mod.Value, right: event_mod.Value) Error!event_mod.Value {
    if (left == .integer and right == .integer) {
        const ov = @subWithOverflow(left.integer, right.integer);
        if (ov[1] != 0) return Error.ArithmeticOverflow;
        return event_mod.Value{ .integer = ov[0] };
    }

    const lf = try toFloat(left);
    const rf = try toFloat(right);
    return event_mod.Value{ .float = lf - rf };
}

fn multiplyValues(left: event_mod.Value, right: event_mod.Value) Error!event_mod.Value {
    if (left == .integer and right == .integer) {
        const ov = @mulWithOverflow(left.integer, right.integer);
        if (ov[1] != 0) return Error.ArithmeticOverflow;
        return event_mod.Value{ .integer = ov[0] };
    }

    const lf = try toFloat(left);
    const rf = try toFloat(right);
    return event_mod.Value{ .float = lf * rf };
}

fn divideValues(left: event_mod.Value, right: event_mod.Value) Error!event_mod.Value {
    const denominator = try toFloat(right);
    if (denominator == 0) return Error.DivideByZero;
    const numerator = try toFloat(left);
    return event_mod.Value{ .float = numerator / denominator };
}

fn logicalAnd(left: event_mod.Value, right: event_mod.Value) Error!bool {
    if (left != .boolean or right != .boolean) return Error.TypeMismatch;
    return left.boolean and right.boolean;
}

fn logicalOr(left: event_mod.Value, right: event_mod.Value) Error!bool {
    if (left != .boolean or right != .boolean) return Error.TypeMismatch;
    return left.boolean or right.boolean;
}

fn toFloat(value: event_mod.Value) Error!f64 {
    return switch (value) {
        .integer => |v| @as(f64, @floatFromInt(v)),
        .float => |v| v,
        else => Error.TypeMismatch,
    };
}

const OrderOp = enum { less, less_equal, greater, greater_equal };

fn compareOrder(op: OrderOp, left: event_mod.Value, right: event_mod.Value) Error!bool {
    if (left == .string and right == .string) {
        const cmp = std.mem.order(u8, left.string, right.string);
        return switch (op) {
            .less => cmp == .lt,
            .less_equal => cmp == .lt or cmp == .eq,
            .greater => cmp == .gt,
            .greater_equal => cmp == .gt or cmp == .eq,
        };
    }

    const lf = try toFloat(left);
    const rf = try toFloat(right);

    return switch (op) {
        .less => lf < rf,
        .less_equal => lf <= rf,
        .greater => lf > rf,
        .greater_equal => lf >= rf,
    };
}

fn compareEqual(left: event_mod.Value, right: event_mod.Value) bool {
    switch (left) {
        .integer => |l| switch (right) {
            .integer => |r| return l == r,
            .float => |r| return @as(f64, @floatFromInt(l)) == r,
            else => return false,
        },
        .float => |l| switch (right) {
            .integer => |r| return l == @as(f64, @floatFromInt(r)),
            .float => |r| return l == r,
            else => return false,
        },
        .boolean => |l| switch (right) {
            .boolean => |r| return l == r,
            else => return false,
        },
        .string => |l| switch (right) {
            .string => |r| return std.mem.eql(u8, l, r),
            else => return false,
        },
        .null => switch (right) {
            .null => return true,
            else => return false,
        },
    }
}

const testing = std.testing;

fn testEvent(allocator: std.mem.Allocator) !event_mod.Event {
    const fields = try allocator.alloc(event_mod.Field, 1);
    fields[0] = .{ .name = "syslog_severity", .value = .{ .integer = 4 } };

    return event_mod.Event{
        .metadata = .{ .source_id = "syslog" },
        .payload = .{
            .log = .{
                .message = "hello",
                .fields = fields,
            },
        },
    };
}

fn identifierBinding(identifier: ast.Identifier) IdentifierBinding {
    return IdentifierBinding{
        .canonical = identifier.canonical,
        .value = identifier.value,
        .quoted = identifier.quoted,
    };
}

fn qualifierMatches(binding: IdentifierBinding, qualifier: ast.Identifier) bool {
    if (binding.quoted or qualifier.quoted) {
        if (binding.quoted != qualifier.quoted) return false;
        return std.mem.eql(u8, binding.value, qualifier.value);
    }
    return std.mem.eql(u8, binding.canonical, qualifier.canonical);
}

fn qualifierBindingsEqual(left: IdentifierBinding, right: IdentifierBinding) bool {
    if (left.quoted or right.quoted) {
        if (left.quoted != right.quoted) return false;
        return std.mem.eql(u8, left.value, right.value);
    }
    return std.mem.eql(u8, left.canonical, right.canonical);
}

fn qualifierAllowed(binding_opt: ?TableBinding, qualifier: ast.Identifier) bool {
    if (binding_opt) |binding| {
        if (qualifierMatches(binding.table, qualifier)) return true;
        if (binding.alias) |alias_binding| {
            if (qualifierMatches(alias_binding, qualifier)) return true;
        }
    }
    return false;
}

fn starQualifierAllowed(binding_opt: ?TableBinding, qualifier: IdentifierBinding) bool {
    if (binding_opt) |binding| {
        if (qualifierBindingsEqual(binding.table, qualifier)) return true;
        if (binding.alias) |alias_binding| {
            if (qualifierBindingsEqual(alias_binding, qualifier)) return true;
        }
    }
    return false;
}

fn determineTableBinding(stmt: *const ast.SelectStatement) Error!?TableBinding {
    if (stmt.from.len == 0) return null;

    const first = stmt.from[0];
    return switch (first.relation) {
        .table => |table_ref| blk: {
            var binding = TableBinding{ .table = identifierBinding(table_ref.name) };
            if (table_ref.alias) |alias_ident| {
                binding.alias = identifierBinding(alias_ident);
            }
            break :blk binding;
        },
        else => Error.UnsupportedFeature,
    };
}

fn freeEvent(allocator: std.mem.Allocator, ev: event_mod.Event) void {
    switch (ev.payload) {
        .log => |log_event| allocator.free(log_event.fields),
    }
}

test "execute projection" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const stmt = try @import("parser.zig").parseSelect(arena, "SELECT syslog_severity, message FROM logs WHERE syslog_severity <= 4");

    var program = try compile(testing.allocator, stmt);
    defer program.deinit();

    var event = try testEvent(testing.allocator);
    defer freeEvent(testing.allocator, event);

    const maybe_row = try program.execute(testing.allocator, &event);
    defer if (maybe_row) |row| row.deinit();
    try testing.expect(maybe_row != null);
    const row = maybe_row.?;
    try testing.expectEqual(@as(usize, 2), row.values.len);
    try testing.expectEqualStrings("syslog_severity", row.values[0].name);
    try testing.expect(row.values[0].value == .integer);
    try testing.expect(row.values[0].value.integer == 4);
    try testing.expectEqualStrings("message", row.values[1].name);
    try testing.expect(row.values[1].value == .string);
    try testing.expectEqualStrings("hello", row.values[1].value.string);
}

test "filter out non matching event" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const stmt = try @import("parser.zig").parseSelect(arena, "SELECT message FROM logs WHERE syslog_severity <= 2");
    var program = try compile(testing.allocator, stmt);
    defer program.deinit();

    var event = try testEvent(testing.allocator);
    defer freeEvent(testing.allocator, event);

    const maybe_row = try program.execute(testing.allocator, &event);
    try testing.expect(maybe_row == null);
}

test "missing column yields null" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const stmt = try @import("parser.zig").parseSelect(arena, "SELECT missing FROM logs");
    var program = try compile(testing.allocator, stmt);
    defer program.deinit();

    var event = try testEvent(testing.allocator);
    defer freeEvent(testing.allocator, event);

    const maybe_row = try program.execute(testing.allocator, &event);
    defer if (maybe_row) |row| row.deinit();

    const row = maybe_row orelse return testing.expect(false);
    try testing.expectEqual(@as(usize, 1), row.values.len);
    try testing.expect(row.values[0].value == .null);
}

test "integer arithmetic overflow yields error" {
    const max = std.math.maxInt(i64);
    try testing.expectError(Error.ArithmeticOverflow, addValues(.{ .integer = max }, .{ .integer = 1 }));
    try testing.expectError(Error.ArithmeticOverflow, subtractValues(.{ .integer = std.math.minInt(i64) }, .{ .integer = 1 }));
    try testing.expectError(Error.ArithmeticOverflow, multiplyValues(.{ .integer = max }, .{ .integer = 2 }));
}

test "execute star projection expands fields" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const stmt = try @import("parser.zig").parseSelect(arena, "SELECT * FROM logs");
    var program = try compile(testing.allocator, stmt);
    defer program.deinit();

    var event = try testEvent(testing.allocator);
    defer freeEvent(testing.allocator, event);

    const maybe_row = try program.execute(testing.allocator, &event);
    defer if (maybe_row) |row| row.deinit();

    const row = maybe_row orelse return testing.expect(false);
    try testing.expectEqual(@as(usize, 2), row.values.len);
    try testing.expectEqualStrings("message", row.values[0].name);
    try testing.expect(row.values[0].value == .string);
    try testing.expectEqualStrings("hello", row.values[0].value.string);
    try testing.expectEqualStrings("syslog_severity", row.values[1].name);
    try testing.expect(row.values[1].value == .integer);
    try testing.expectEqual(@as(i64, 4), row.values[1].value.integer);
}

test "resolve qualified column reference" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const stmt = try @import("parser.zig").parseSelect(arena, "SELECT logs.message FROM logs");
    var program = try compile(testing.allocator, stmt);
    defer program.deinit();

    var event = try testEvent(testing.allocator);
    defer freeEvent(testing.allocator, event);

    const maybe_row = try program.execute(testing.allocator, &event);
    defer if (maybe_row) |row| row.deinit();

    const row = maybe_row orelse return testing.expect(false);
    try testing.expectEqual(@as(usize, 1), row.values.len);
    try testing.expectEqualStrings("message", row.values[0].name);
    try testing.expect(row.values[0].value == .string);
    try testing.expectEqualStrings("hello", row.values[0].value.string);
}

test "unknown table qualifier yields error" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const stmt = try @import("parser.zig").parseSelect(arena, "SELECT foo.message FROM logs");
    var program = try compile(testing.allocator, stmt);
    defer program.deinit();

    var event = try testEvent(testing.allocator);
    defer freeEvent(testing.allocator, event);

    try testing.expectError(Error.UnknownColumn, program.execute(testing.allocator, &event));
}

test "unknown star qualifier yields error" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const stmt = try @import("parser.zig").parseSelect(arena, "SELECT foo.* FROM logs");
    var program = try compile(testing.allocator, stmt);
    defer program.deinit();

    var event = try testEvent(testing.allocator);
    defer freeEvent(testing.allocator, event);

    try testing.expectError(Error.UnknownColumn, program.execute(testing.allocator, &event));
}

test "execute mixed star and expression projection" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const stmt = try @import("parser.zig").parseSelect(arena, "SELECT *, syslog_severity + 1 AS severity_plus_one FROM logs");
    var program = try compile(testing.allocator, stmt);
    defer program.deinit();

    var event = try testEvent(testing.allocator);
    defer freeEvent(testing.allocator, event);

    const maybe_row = try program.execute(testing.allocator, &event);
    defer if (maybe_row) |row| row.deinit();

    const row = maybe_row orelse return testing.expect(false);
    try testing.expectEqual(@as(usize, 3), row.values.len);
    try testing.expectEqualStrings("message", row.values[0].name);
    try testing.expectEqualStrings("syslog_severity", row.values[1].name);
    try testing.expectEqualStrings("severity_plus_one", row.values[2].name);
    try testing.expect(row.values[2].value == .integer);
    try testing.expectEqual(@as(i64, 5), row.values[2].value.integer);
}
