const std = @import("std");
const ast = @import("ast.zig");
const source_mod = @import("source");
const event_mod = source_mod.event;

/// Error set shared with the runtime evaluator.
pub const Error = std.mem.Allocator.Error || error{
    UnsupportedFeature,
    UnknownColumn,
    TypeMismatch,
    UnsupportedFunction,
    DivideByZero,
    ArithmeticOverflow,
};

/// Describes the value domain produced by an expression.
pub const ValueType = enum {
    value,
    boolean,
};

/// Maximum stack depth supported by the VM without additional allocations.
pub const max_stack_limit = 128;

/// Descriptor allowing the VM to access event-scoped columns without referencing
/// the runtime module (to avoid circular dependencies).
pub const EventBinding = union(enum) {
    message,
    source_id,
    field: FieldBinding,
};

pub const FieldBinding = struct {
    name: []const u8,
    canonical: []const u8,
    quoted: bool,
};

/// Instruction stream executed by the VM. Each instruction manipulates a value
/// stack containing `event_mod.Value` instances.
pub const Instruction = union(enum) {
    push_literal: Literal,
    push_event: EventBinding,
    push_group_key: usize,
    push_aggregate: usize,
    unary: UnaryOp,
    binary: BinaryOp,
    jump_if_false: Jump,
    jump_if_true: Jump,
};

pub const Literal = union(enum) {
    string: []const u8,
    integer: i64,
    float: f64,
    boolean: bool,
    null,
};

pub const UnaryOp = enum {
    plus,
    minus,
    logical_not,
};

pub const BinaryOp = enum {
    add,
    subtract,
    multiply,
    divide,
    equal,
    not_equal,
    less,
    less_equal,
    greater,
    greater_equal,
    logical_and,
    logical_or,
};

pub const Jump = struct {
    /// Instruction index to branch to when the condition is met.
    target: u32,
};

/// Compiled IR program with pre-computed stack requirements.
pub const Program = struct {
    instructions: []Instruction,
    result_type: ValueType,
    max_stack: u8,

    pub fn deinit(self: Program, allocator: std.mem.Allocator) void {
        allocator.free(self.instructions);
    }
};

pub const CompileOptions = struct {
    allow_event_columns: bool = false,
    allow_group_columns: bool = false,
    allow_aggregate_refs: bool = false,
    expect_boolean_result: bool = false,
};

pub const Environment = struct {
    event_resolver: ?EventResolver = null,
    group_resolver: ?GroupResolver = null,
    aggregate_resolver: ?AggregateResolver = null,
};

pub const EventResolver = struct {
    context: *anyopaque,
    resolve_fn: *const fn (context: *anyopaque, column: ast.ColumnRef) Error!EventBinding,
};

pub const GroupResolver = struct {
    context: *anyopaque,
    resolve_fn: *const fn (context: *anyopaque, column: ast.ColumnRef) Error!?usize,
};

pub const AggregateResolver = struct {
    context: *anyopaque,
    resolve_fn: *const fn (context: *anyopaque, call: *const ast.FunctionCall) Error!?usize,
};

/// Interface passed to the VM for loading data that resides outside of the
/// instruction stream.
pub const ExecutionContext = struct {
    event_accessor: ?EventAccessor = null,
    group_values: []const event_mod.Value = &.{},
    aggregate_accessor: ?AggregateAccessor = null,
};

pub const EventAccessor = struct {
    context: *anyopaque,
    load_fn: *const fn (context: *anyopaque, binding: EventBinding) Error!event_mod.Value,
};

pub const AggregateAccessor = struct {
    context: *anyopaque,
    load_fn: *const fn (context: *anyopaque, aggregate_index: usize) Error!event_mod.Value,
};

const ValueStack = struct {
    buffer: []event_mod.Value,
    len: usize = 0,

    fn init(storage: []event_mod.Value) ValueStack {
        return .{ .buffer = storage, .len = 0 };
    }

    fn push(self: *ValueStack, value: event_mod.Value) Error!void {
        if (self.len >= self.buffer.len) return Error.UnsupportedFeature;
        self.buffer[self.len] = value;
        self.len += 1;
    }

    fn peek(self: *ValueStack) Error!*event_mod.Value {
        if (self.len == 0) return Error.UnsupportedFeature;
        return &self.buffer[self.len - 1];
    }

    fn pop(self: *ValueStack) Error!event_mod.Value {
        if (self.len == 0) return Error.UnsupportedFeature;
        self.len -= 1;
        return self.buffer[self.len];
    }

    fn replaceTop(self: *ValueStack, value: event_mod.Value) Error!void {
        if (self.len == 0) return Error.UnsupportedFeature;
        self.buffer[self.len - 1] = value;
    }
};

/// Executes the program and returns the value produced by the final instruction.
pub fn execute(
    program: *const Program,
    context: ExecutionContext,
) Error!event_mod.Value {
    if (program.max_stack > max_stack_limit)
        return Error.UnsupportedFeature;

    var storage: [max_stack_limit]event_mod.Value = undefined;
    var stack = ValueStack.init(storage[0..program.max_stack]);

    var ip: usize = 0;
    while (ip < program.instructions.len) : (ip += 1) {
        const instr = program.instructions[ip];
        switch (instr) {
            .push_literal => |literal| try stack.push(valueFromLiteral(literal)),
            .push_event => |binding| {
                const accessor = context.event_accessor orelse return Error.UnsupportedFeature;
                const value = try accessor.load_fn(accessor.context, binding);
                try stack.push(value);
            },
            .push_group_key => |index| {
                if (index >= context.group_values.len) return Error.UnknownColumn;
                try stack.push(context.group_values[index]);
            },
            .push_aggregate => |index| {
                const accessor = context.aggregate_accessor orelse return Error.UnsupportedFeature;
                const value = try accessor.load_fn(accessor.context, index);
                try stack.push(value);
            },
            .unary => |op| try applyUnary(&stack, op),
            .binary => |op| try applyBinary(&stack, op),
            .jump_if_false => |jump| {
                const top_ptr = try stack.peek();
                if (top_ptr.* != .boolean) return Error.TypeMismatch;
                if (!top_ptr.boolean) {
                    ip = jump.target;
                    continue;
                }
                _ = try stack.pop();
            },
            .jump_if_true => |jump| {
                const top_ptr = try stack.peek();
                if (top_ptr.* != .boolean) return Error.TypeMismatch;
                if (top_ptr.boolean) {
                    ip = jump.target;
                    continue;
                }
                _ = try stack.pop();
            },
        }
    }

    if (stack.len != 1) return Error.UnsupportedFeature;
    return stack.pop();
}

fn valueFromLiteral(literal: Literal) event_mod.Value {
    return switch (literal) {
        .string => |text| event_mod.Value{ .string = text },
        .integer => |value| event_mod.Value{ .integer = value },
        .float => |value| event_mod.Value{ .float = value },
        .boolean => |flag| event_mod.Value{ .boolean = flag },
        .null => event_mod.Value{ .null = {} },
    };
}

fn applyUnary(stack: *ValueStack, op: UnaryOp) Error!void {
    switch (op) {
        .plus => {},
        .minus => {
            const operand_ptr = try stack.peek();
            switch (operand_ptr.*) {
                .integer => |value| operand_ptr.* = event_mod.Value{ .integer = -value },
                .float => |value| operand_ptr.* = event_mod.Value{ .float = -value },
                else => return Error.TypeMismatch,
            }
        },
        .logical_not => {
            const operand_ptr = try stack.peek();
            if (operand_ptr.* != .boolean) return Error.TypeMismatch;
            operand_ptr.* = event_mod.Value{ .boolean = !operand_ptr.boolean };
        },
    }
}

fn applyBinary(stack: *ValueStack, op: BinaryOp) Error!void {
    switch (op) {
        .add, .subtract, .multiply, .divide => {
            const right = try stack.pop();
            const left_ptr = try stack.peek();
            switch (op) {
                .add => left_ptr.* = try addValues(left_ptr.*, right),
                .subtract => left_ptr.* = try subtractValues(left_ptr.*, right),
                .multiply => left_ptr.* = try multiplyValues(left_ptr.*, right),
                .divide => left_ptr.* = try divideValues(left_ptr.*, right),
                else => unreachable,
            }
        },
        .equal, .not_equal, .less, .less_equal, .greater, .greater_equal => {
            const right = try stack.pop();
            const left_ptr = try stack.peek();
            const result = switch (op) {
                .equal => event_mod.Value{ .boolean = compareEqual(left_ptr.*, right) },
                .not_equal => event_mod.Value{ .boolean = !compareEqual(left_ptr.*, right) },
                .less => event_mod.Value{ .boolean = try compareOrder(.less, left_ptr.*, right) },
                .less_equal => event_mod.Value{ .boolean = try compareOrder(.less_equal, left_ptr.*, right) },
                .greater => event_mod.Value{ .boolean = try compareOrder(.greater, left_ptr.*, right) },
                .greater_equal => event_mod.Value{ .boolean = try compareOrder(.greater_equal, left_ptr.*, right) },
                else => unreachable,
            };
            left_ptr.* = result;
        },
        .logical_and, .logical_or => unreachable, // handled through jumps
    }
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

const OrderOp = enum { less, less_equal, greater, greater_equal };

fn compareOrder(op: OrderOp, left: event_mod.Value, right: event_mod.Value) Error!bool {
    if (left == .null or right == .null) return false;

    if (left == .string and right == .string) {
        const cmp = std.mem.order(u8, left.string, right.string);
        return switch (op) {
            .less => cmp == .lt,
            .less_equal => cmp == .lt or cmp == .eq,
            .greater => cmp == .gt,
            .greater_equal => cmp == .gt or cmp == .eq,
        };
    }

    if (left == .integer and right == .integer) {
        const li = left.integer;
        const ri = right.integer;
        return switch (op) {
            .less => li < ri,
            .less_equal => li <= ri,
            .greater => li > ri,
            .greater_equal => li >= ri,
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
            .float => |r| {
                if (std.math.isNan(l) and std.math.isNan(r)) return true;
                return l == r;
            },
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

fn toFloat(value: event_mod.Value) Error!f64 {
    return switch (value) {
        .integer => |v| @as(f64, @floatFromInt(v)),
        .float => |v| v,
        else => Error.TypeMismatch,
    };
}

pub fn compileExpression(
    allocator: std.mem.Allocator,
    expr: *const ast.Expression,
    options: CompileOptions,
    env: Environment,
) Error!Program {
    const depth = computeStackDepth(expr);
    if (depth == 0 or depth > max_stack_limit) return Error.UnsupportedFeature;

    var compiler = Compiler{
        .allocator = allocator,
        .env = env,
        .options = options,
        .instructions = std.ArrayListUnmanaged(Instruction){},
    };
    defer compiler.instructions.deinit(allocator);

    const inferred_type = try compiler.emitExpression(expr);

    const program_type = if (options.expect_boolean_result) ValueType.boolean else inferred_type;

    const instructions = try compiler.instructions.toOwnedSlice(allocator);
    return Program{
        .instructions = instructions,
        .result_type = program_type,
        .max_stack = @intCast(depth),
    };
}

fn computeStackDepth(expr: *const ast.Expression) usize {
    return switch (expr.*) {
        .literal, .column, .function_call => 1,
        .unary => |un| computeStackDepth(un.operand),
        .binary => |bin| {
            const left = computeStackDepth(bin.left);
            const right = computeStackDepth(bin.right);
            return switch (bin.op) {
                .logical_and, .logical_or => @max(left, right),
                else => @max(left, 1 + right),
            };
        },
        .star => 0,
    };
}

const Compiler = struct {
    allocator: std.mem.Allocator,
    env: Environment,
    options: CompileOptions,
    instructions: std.ArrayListUnmanaged(Instruction),

    fn emitExpression(self: *Compiler, expr: *const ast.Expression) Error!ValueType {
        return switch (expr.*) {
            .literal => |lit| {
                const literal = try literalFromAst(lit);
                _ = try self.append(.{ .push_literal = literal });
                return literalValueType(literal);
            },
            .column => |col| try self.compileColumn(col),
            .unary => |un| try self.compileUnary(un),
            .binary => |bin| try self.compileBinary(bin),
            .function_call => |*call| try self.compileFunctionCall(call),
            .star => Error.UnsupportedFeature,
        };
    }

    fn compileColumn(self: *Compiler, col: ast.ColumnRef) Error!ValueType {
        if (self.options.allow_group_columns) {
            if (self.env.group_resolver) |resolver| {
                if (try resolver.resolve_fn(resolver.context, col)) |index| {
                    _ = try self.append(.{ .push_group_key = index });
                    return ValueType.value;
                }
            }
        }

        if (!self.options.allow_event_columns) return Error.UnknownColumn;

        const resolver = self.env.event_resolver orelse return Error.UnsupportedFeature;
        const binding = try resolver.resolve_fn(resolver.context, col);
        _ = try self.append(.{ .push_event = binding });
        return ValueType.value;
    }

    fn compileUnary(self: *Compiler, un: ast.UnaryExpr) Error!ValueType {
        const operand_type = try self.emitExpression(un.operand);
        switch (un.op) {
            .plus => {
                _ = try self.append(.{ .unary = .plus });
                return operand_type;
            },
            .minus => {
                _ = try self.append(.{ .unary = .minus });
                return operand_type;
            },
            .not => {
                _ = try self.append(.{ .unary = .logical_not });
                return ValueType.boolean;
            },
        }
    }

    fn compileBinary(self: *Compiler, bin: ast.BinaryExpr) Error!ValueType {
        const op = bin.op;
        switch (op) {
            .logical_and => {
                _ = try self.emitExpression(bin.left);
                const jump_index = try self.append(.{ .jump_if_false = .{ .target = 0 } });
                _ = try self.emitExpression(bin.right);
                self.patchJump(jump_index, self.instructions.items.len);
                return ValueType.boolean;
            },
            .logical_or => {
                _ = try self.emitExpression(bin.left);
                const jump_index = try self.append(.{ .jump_if_true = .{ .target = 0 } });
                _ = try self.emitExpression(bin.right);
                self.patchJump(jump_index, self.instructions.items.len);
                return ValueType.boolean;
            },
            else => {
                _ = try self.emitExpression(bin.left);
                _ = try self.emitExpression(bin.right);
                _ = try self.append(.{ .binary = binaryFromAst(op) });
                return switch (op) {
                    .add, .subtract, .multiply, .divide => ValueType.value,
                    .equal, .not_equal, .less, .less_equal, .greater, .greater_equal => ValueType.boolean,
                    else => unreachable,
                };
            },
        }
    }

    fn compileFunctionCall(self: *Compiler, call: *const ast.FunctionCall) Error!ValueType {
        if (!self.options.allow_aggregate_refs) return Error.UnsupportedFunction;
        const resolver = self.env.aggregate_resolver orelse return Error.UnsupportedFunction;
        const index = (try resolver.resolve_fn(resolver.context, call)) orelse return Error.UnsupportedFunction;
        _ = try self.append(.{ .push_aggregate = index });
        return ValueType.value;
    }

    fn append(self: *Compiler, instruction: Instruction) Error!usize {
        const idx = self.instructions.items.len;
        try self.instructions.append(self.allocator, instruction);
        return idx;
    }

    fn patchJump(self: *Compiler, instruction_index: usize, target_index: usize) void {
        const instr = &self.instructions.items[instruction_index];
        switch (instr.*) {
            .jump_if_false => |*jump| jump.target = @intCast(target_index),
            .jump_if_true => |*jump| jump.target = @intCast(target_index),
            else => {},
        }
    }
};

fn literalFromAst(lit: ast.Literal) Error!Literal {
    return switch (lit.value) {
        .integer => |value| Literal{ .integer = value },
        .float => |text| blk: {
            const parsed = std.fmt.parseFloat(f64, text) catch return Error.TypeMismatch;
            break :blk Literal{ .float = parsed };
        },
        .string => |text| Literal{ .string = text },
        .boolean => |flag| Literal{ .boolean = flag },
        .null => Literal.null,
    };
}

fn literalValueType(lit: Literal) ValueType {
    return switch (lit) {
        .boolean => ValueType.boolean,
        else => ValueType.value,
    };
}

fn binaryFromAst(op: ast.BinaryOperator) BinaryOp {
    return switch (op) {
        .add => .add,
        .subtract => .subtract,
        .multiply => .multiply,
        .divide => .divide,
        .equal => .equal,
        .not_equal => .not_equal,
        .less => .less,
        .less_equal => .less_equal,
        .greater => .greater,
        .greater_equal => .greater_equal,
        .logical_and, .logical_or => unreachable,
    };
}
