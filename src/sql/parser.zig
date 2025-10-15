const std = @import("std");
const ast = @import("ast.zig");
const lexmod = @import("lexer.zig");
const operators = @import("operators.zig");

pub const ParseError = error{
    UnexpectedToken,
    ExpectedToken,
    OutOfMemory,
    InvalidNumber,
    LexError,
};

pub const Scratch = struct {
    arena: std.heap.ArenaAllocator,

    pub fn init(backing: std.mem.Allocator) Scratch {
        return Scratch{ .arena = std.heap.ArenaAllocator.init(backing) };
    }

    pub fn deinit(self: *Scratch) void {
        self.arena.deinit();
    }

    pub fn allocator(self: *Scratch) std.mem.Allocator {
        return self.arena.allocator();
    }

    pub fn reset(self: *Scratch) void {
        _ = self.arena.reset(.retain_capacity);
    }
};

const BinaryOpInfo = struct {
    op: ast.BinaryOperator,
    precedence: u8,
};

const CallArguments = struct {
    arguments: []const *const ast.Expression,
    distinct: bool,
    closing_span: lexmod.Span,
};

const IdentifierListResult = struct {
    identifiers: []const ast.Identifier,
    closing_span: lexmod.Span,
};

const unary_precedence: u8 = 60;

const StringPool = std.StringHashMapUnmanaged([]const u8);
const TempList = std.array_list.Managed;

fn spanFrom(start: usize, end: usize) lexmod.Span {
    return .{ .start = start, .end = end };
}

inline fn max(a: usize, b: usize) usize {
    return if (a > b) a else b;
}

inline fn min(a: usize, b: usize) usize {
    return if (a < b) a else b;
}

pub fn parseSelect(arena: std.mem.Allocator, source: []const u8) ParseError!*const ast.SelectStatement {
    var scratch = Scratch.init(std.heap.page_allocator);
    defer scratch.deinit();
    return parseSelectWithScratch(arena, &scratch, source);
}

pub fn parseSelectWithScratch(
    arena: std.mem.Allocator,
    scratch: *Scratch,
    source: []const u8,
) ParseError!*const ast.SelectStatement {
    scratch.reset();

    var parser = try Parser.init(arena, scratch.allocator(), source);
    defer {
        parser.deinit();
        scratch.reset();
    }

    const stmt = try parser.parseSelect();
    try parser.consumeStatementTerminator();
    try parser.expectEof();
    return stmt;
}

pub const Parser = struct {
    arena: std.mem.Allocator,
    scratch: std.mem.Allocator,
    lex: lexmod.Lexer,
    current: lexmod.Token,
    pool: StringPool = .{},
    canonical_pool: StringPool = .{},
    source: []const u8,

    fn init(arena: std.mem.Allocator, scratch: std.mem.Allocator, source: []const u8) ParseError!Parser {
        var owned_source: []const u8 = &[_]u8{};
        if (source.len != 0) {
            const buffer = try arena.alloc(u8, source.len);
            std.mem.copyForwards(u8, buffer, source);
            owned_source = buffer;
        }
        var p = Parser{
            .arena = arena,
            .scratch = scratch,
            .lex = lexmod.Lexer.init(owned_source),
            .current = undefined,
            .pool = .{},
            .source = owned_source,
        };
        try p.pool.ensureTotalCapacity(p.scratch, 32);
        try p.canonical_pool.ensureTotalCapacity(p.scratch, 32);
        try p.advance();
        return p;
    }

    fn deinit(self: *Parser) void {
        self.pool.deinit(self.scratch);
        self.canonical_pool.deinit(self.scratch);
    }

    fn copySlice(self: *Parser, comptime T: type, items: []const T) ParseError![]const T {
        if (items.len == 0) return &[_]T{};
        const dest = try self.arena.alloc(T, items.len);
        std.mem.copyForwards(T, dest, items);
        return dest;
    }

    fn parseSelect(self: *Parser) ParseError!*const ast.SelectStatement {
        var stmt_start = self.current.span.start;
        var last_end: usize = stmt_start;

        var with_clause: ?ast.WithClause = null;
        if (self.current.kind == .keyword and self.current.kind.keyword == .With) {
            const with_start = self.current.span.start;
            try self.advance();
            const clause = try self.parseWithClauseRest(with_start);
            stmt_start = with_start;
            last_end = clause.span.end;
            with_clause = clause;
        }

        if (!(self.current.kind == .keyword and self.current.kind.keyword == .Select)) {
            return ParseError.ExpectedToken;
        }
        const select_span = self.current.span;
        try self.advance();
        last_end = max(last_end, select_span.end);

        var distinct = false;
        var distinct_span: ?lexmod.Span = null;
        if (self.current.kind == .keyword and self.current.kind.keyword == .Distinct) {
            distinct = true;
            distinct_span = self.current.span;
            last_end = max(last_end, self.current.span.end);
            try self.advance();
        }

        var projection_builder = TempList(ast.SelectItem).init(self.scratch);
        defer projection_builder.deinit();
        while (true) {
            const item = try self.parseSelectItem();
            try projection_builder.append(item);
            last_end = max(last_end, item.span.end);
            if (self.current.kind == .symbol and self.current.kind.symbol == .comma) {
                last_end = max(last_end, self.current.span.end);
                try self.advance();
            } else break;
        }
        const projection = try self.copySlice(ast.SelectItem, projection_builder.items);

        var from_tables: []const ast.TableExpression = &[_]ast.TableExpression{};
        if (self.current.kind == .keyword and self.current.kind.keyword == .From) {
            const from_span = self.current.span;
            try self.advance();
            from_tables = try self.parseFromClause();
            if (from_tables.len > 0) {
                last_end = max(last_end, from_tables[from_tables.len - 1].span.end);
            } else {
                last_end = max(last_end, from_span.end);
            }
        }

        var selection: ?*const ast.Expression = null;
        if (self.current.kind == .keyword and self.current.kind.keyword == .Where) {
            const where_start = self.current.span.start;
            try self.advance();
            const expr = try self.parseExpression(0);
            selection = expr;
            last_end = max(last_end, ast.expressionSpan(expr).end);
            stmt_start = min(stmt_start, where_start);
        }

        var group_by_exprs: []const *const ast.Expression = &[_]*const ast.Expression{};
        if (self.isKeyword(.Group)) {
            group_by_exprs = try self.parseGroupByClause();
            if (group_by_exprs.len > 0) {
                last_end = max(last_end, ast.expressionSpan(group_by_exprs[group_by_exprs.len - 1]).end);
            }
        }

        var having_expr: ?*const ast.Expression = null;
        if (self.current.kind == .keyword and self.current.kind.keyword == .Having) {
            const having_start = self.current.span.start;
            try self.advance();
            const expr = try self.parseExpression(0);
            having_expr = expr;
            last_end = max(last_end, ast.expressionSpan(expr).end);
            stmt_start = min(stmt_start, having_start);
        }

        var order_items: []const ast.OrderByItem = &[_]ast.OrderByItem{};
        var order_clause_span: ?lexmod.Span = null;
        if (self.isKeyword(.Order)) {
            const order_start = self.current.span.start;
            order_items = try self.parseOrderByClause();
            if (order_items.len > 0) {
                const order_end = order_items[order_items.len - 1].span.end;
                last_end = max(last_end, order_end);
                order_clause_span = spanFrom(order_start, order_end);
            }
        }

        const stmt = try self.arena.create(ast.SelectStatement);
        stmt.* = .{
            .with_clause = with_clause,
            .distinct = distinct,
            .distinct_span = distinct_span,
            .projection = projection,
            .from = from_tables,
            .selection = selection,
            .group_by = group_by_exprs,
            .having = having_expr,
            .order_by = order_items,
            .order_by_span = order_clause_span,
            .span = spanFrom(stmt_start, last_end),
        };
        return stmt;
    }

    fn parseWithClauseRest(self: *Parser, with_start: usize) ParseError!ast.WithClause {
        var recursive = false;
        var last_end = with_start;
        if (self.current.kind == .keyword and self.current.kind.keyword == .Recursive) {
            recursive = true;
            last_end = max(last_end, self.current.span.end);
            try self.advance();
        }

        var cte_builder = TempList(ast.CommonTableExpression).init(self.scratch);
        defer cte_builder.deinit();

        while (true) {
            const name = try self.expectIdentifier();
            const cte_start = name.span.start;
            var cte_end = name.span.end;

            var columns: []const ast.Identifier = &[_]ast.Identifier{};
            if (self.current.kind == .symbol and self.current.kind.symbol == .l_paren) {
                try self.advance();
                const columns_res = try self.parseIdentifierListBody();
                columns = columns_res.identifiers;
                cte_end = max(cte_end, columns_res.closing_span.end);
            }

            if (!(self.current.kind == .keyword and self.current.kind.keyword == .As)) {
                return ParseError.ExpectedToken;
            }
            try self.advance();

            if (!(self.current.kind == .symbol and self.current.kind.symbol == .l_paren)) {
                return ParseError.ExpectedToken;
            }
            try self.advance();

            const subquery = try self.parseSelect();
            cte_end = max(cte_end, subquery.span.end);

            if (!(self.current.kind == .symbol and self.current.kind.symbol == .r_paren)) {
                return ParseError.ExpectedToken;
            }
            const close_span = self.current.span;
            try self.advance();
            cte_end = max(cte_end, close_span.end);

            try cte_builder.append(.{
                .name = name,
                .columns = columns,
                .query = subquery,
                .span = spanFrom(cte_start, cte_end),
            });
            last_end = max(last_end, cte_end);

            if (self.current.kind == .symbol and self.current.kind.symbol == .comma) {
                last_end = max(last_end, self.current.span.end);
                try self.advance();
            } else break;
        }

        const ctes = try self.copySlice(ast.CommonTableExpression, cte_builder.items);
        return ast.WithClause{
            .recursive = recursive,
            .ctes = ctes,
            .span = spanFrom(with_start, last_end),
        };
    }

    fn parseSelectItem(self: *Parser) ParseError!ast.SelectItem {
        const expr = try self.parseExpression(0);
        const expr_span = ast.expressionSpan(expr);
        var end_index = expr_span.end;

        var alias: ?ast.Identifier = null;
        if (self.current.kind == .keyword and self.current.kind.keyword == .As) {
            try self.advance();
            alias = try self.expectIdentifier();
            end_index = alias.?.span.end;
        }

        return switch (expr.*) {
            .star => |star| ast.SelectItem{ .kind = .{ .star = star }, .alias = alias, .span = spanFrom(expr_span.start, end_index) },
            else => ast.SelectItem{ .kind = .{ .expression = expr }, .alias = alias, .span = spanFrom(expr_span.start, end_index) },
        };
    }

    fn parseFromClause(self: *Parser) ParseError![]const ast.TableExpression {
        var table_builder = TempList(ast.TableExpression).init(self.scratch);
        defer table_builder.deinit();

        while (true) {
            const relation = try self.parseRelation();
            const relation_span = ast.relationSpan(relation);
            const expr_start = relation_span.start;
            var expr_end = relation_span.end;

            var joins_slice: []const ast.Join = &[_]ast.Join{};
            {
                var join_builder = TempList(ast.Join).init(self.scratch);
                defer join_builder.deinit();
                while (true) {
                    const join_opt = try self.maybeParseJoin();
                    if (join_opt) |join_val| {
                        expr_end = max(expr_end, join_val.span.end);
                        try join_builder.append(join_val);
                    } else break;
                }
                if (join_builder.items.len > 0) {
                    joins_slice = try self.copySlice(ast.Join, join_builder.items);
                }
            }

            try table_builder.append(.{
                .relation = relation,
                .joins = joins_slice,
                .span = spanFrom(expr_start, expr_end),
            });

            if (self.current.kind == .symbol and self.current.kind.symbol == .comma) {
                try self.advance();
            } else break;
        }

        return try self.copySlice(ast.TableExpression, table_builder.items);
    }

    fn parseRelation(self: *Parser) ParseError!ast.Relation {
        if (self.current.kind == .symbol and self.current.kind.symbol == .l_paren) {
            const start = self.current.span.start;
            try self.advance();
            const subquery = try self.parseSelect();
            if (!(self.current.kind == .symbol and self.current.kind.symbol == .r_paren)) {
                return ParseError.ExpectedToken;
            }
            const closing_span = self.current.span;
            try self.advance();
            var end_index = max(subquery.span.end, closing_span.end);
            var alias: ast.Identifier = undefined;
            if (self.current.kind == .keyword and self.current.kind.keyword == .As) {
                try self.advance();
            }
            alias = try self.expectIdentifier();
            end_index = max(end_index, alias.span.end);
            return .{ .derived = .{ .subquery = subquery, .alias = alias, .span = spanFrom(start, end_index) } };
        }

        const name = try self.expectIdentifier();
        var alias: ?ast.Identifier = null;
        var end_index = name.span.end;
        if (self.current.kind == .keyword and self.current.kind.keyword == .As) {
            try self.advance();
            const alias_ident = try self.expectIdentifier();
            alias = alias_ident;
            end_index = alias_ident.span.end;
        } else if (self.current.kind == .identifier) {
            const alias_ident = try self.expectIdentifier();
            alias = alias_ident;
            end_index = alias_ident.span.end;
        }
        return .{ .table = .{ .name = name, .alias = alias, .span = spanFrom(name.span.start, end_index) } };
    }

    fn maybeParseJoin(self: *Parser) ParseError!?ast.Join {
        if (!(self.isKeyword(.Join) or self.isKeyword(.Left) or self.isKeyword(.Inner))) {
            return null;
        }

        const start = self.current.span.start;
        var end_index = start;
        var kind: ast.JoinKind = .inner;

        if (self.current.kind == .keyword and self.current.kind.keyword == .Join) {
            end_index = max(end_index, self.current.span.end);
            try self.advance();
        } else if (self.current.kind == .keyword and self.current.kind.keyword == .Left) {
            kind = .left;
            end_index = max(end_index, self.current.span.end);
            try self.advance();
            if (self.current.kind == .keyword and self.current.kind.keyword == .Outer) {
                end_index = max(end_index, self.current.span.end);
                try self.advance();
            }
            if (!(self.current.kind == .keyword and self.current.kind.keyword == .Join)) {
                return ParseError.ExpectedToken;
            }
            end_index = max(end_index, self.current.span.end);
            try self.advance();
        } else if (self.current.kind == .keyword and self.current.kind.keyword == .Inner) {
            end_index = max(end_index, self.current.span.end);
            try self.advance();
            if (!(self.current.kind == .keyword and self.current.kind.keyword == .Join)) {
                return ParseError.ExpectedToken;
            }
            end_index = max(end_index, self.current.span.end);
            try self.advance();
        }

        const relation = try self.parseRelation();
        const relation_span = ast.relationSpan(relation);
        end_index = max(end_index, relation_span.end);

        var constraint: ast.JoinConstraint = .none;
        if (self.current.kind == .keyword and self.current.kind.keyword == .On) {
            try self.advance();
            const expr = try self.parseExpression(0);
            constraint = .{ .on = expr };
            end_index = max(end_index, ast.expressionSpan(expr).end);
        } else if (self.current.kind == .keyword and self.current.kind.keyword == .Using) {
            try self.advance();
            if (!(self.current.kind == .symbol and self.current.kind.symbol == .l_paren)) {
                return ParseError.ExpectedToken;
            }
            try self.advance();
            const columns_res = try self.parseIdentifierListBody();
            constraint = .{ .using = columns_res.identifiers };
            end_index = max(end_index, columns_res.closing_span.end);
        }

        return ast.Join{
            .kind = kind,
            .relation = relation,
            .constraint = constraint,
            .span = spanFrom(start, end_index),
        };
    }

    fn parseGroupByClause(self: *Parser) ParseError![]const *const ast.Expression {
        try self.expectKeyword(.Group);
        try self.expectKeyword(.By);

        var expr_builder = TempList(*const ast.Expression).init(self.scratch);
        defer expr_builder.deinit();

        while (true) {
            const expr = try self.parseExpression(0);
            try expr_builder.append(expr);
            if (!(try self.matchSymbol(.comma))) break;
        }

        return try self.copySlice(*const ast.Expression, expr_builder.items);
    }

    fn parseOrderByClause(self: *Parser) ParseError![]const ast.OrderByItem {
        try self.expectKeyword(.Order);
        try self.expectKeyword(.By);

        var item_builder = TempList(ast.OrderByItem).init(self.scratch);
        defer item_builder.deinit();

        while (true) {
            const expr = try self.parseExpression(0);
            const expr_span = ast.expressionSpan(expr);
            var item_end = expr_span.end;
            var direction = ast.SortDirection.asc;
            if (self.current.kind == .keyword and self.current.kind.keyword == .Asc) {
                direction = .asc;
                item_end = max(item_end, self.current.span.end);
                try self.advance();
            } else if (self.current.kind == .keyword and self.current.kind.keyword == .Desc) {
                direction = .desc;
                item_end = max(item_end, self.current.span.end);
                try self.advance();
            }

            try item_builder.append(.{ .expr = expr, .direction = direction, .span = spanFrom(expr_span.start, item_end) });

            if (self.current.kind == .symbol and self.current.kind.symbol == .comma) {
                try self.advance();
            } else break;
        }

        return try self.copySlice(ast.OrderByItem, item_builder.items);
    }

    fn parseIdentifierListBody(self: *Parser) ParseError!IdentifierListResult {
        var builder = TempList(ast.Identifier).init(self.scratch);
        defer builder.deinit();

        while (true) {
            const ident = try self.expectIdentifier();
            try builder.append(ident);
            if (self.current.kind == .symbol and self.current.kind.symbol == .comma) {
                try self.advance();
            } else break;
        }
        if (!(self.current.kind == .symbol and self.current.kind.symbol == .r_paren)) {
            return ParseError.ExpectedToken;
        }
        const close_span = self.current.span;
        try self.advance();

        return IdentifierListResult{
            .identifiers = try self.copySlice(ast.Identifier, builder.items),
            .closing_span = close_span,
        };
    }

    fn parseExpression(self: *Parser, min_prec: u8) ParseError!*const ast.Expression {
        var left = try self.parsePrefix();

        while (true) {
            const op_info_opt = self.peekBinaryOperator();
            if (op_info_opt == null) break;
            const op_info = op_info_opt.?;
            if (op_info.precedence < min_prec) break;

            try self.advance();
            const right = try self.parseExpression(op_info.precedence + 1);
            left = try self.makeBinary(op_info.op, left, right);
        }

        return left;
    }

    fn parsePrefix(self: *Parser) ParseError!*const ast.Expression {
        if (self.current.kind == .keyword and self.current.kind.keyword == .Not) {
            const op_start = self.current.span.start;
            try self.advance();
            const operand = try self.parseExpression(unary_precedence);
            return self.makeUnary(.not, op_start, operand);
        }

        if (self.current.kind == .symbol) {
            switch (self.current.kind.symbol) {
                .plus => {
                    const op_start = self.current.span.start;
                    try self.advance();
                    const operand = try self.parseExpression(unary_precedence);
                    return self.makeUnary(.plus, op_start, operand);
                },
                .minus => {
                    const op_start = self.current.span.start;
                    try self.advance();
                    const operand = try self.parseExpression(unary_precedence);
                    return self.makeUnary(.minus, op_start, operand);
                },
                else => {},
            }
        }

        return self.parsePrimary();
    }

    fn parsePrimary(self: *Parser) ParseError!*const ast.Expression {
        const tok = self.current;
        switch (tok.kind) {
            .identifier => |name_slice| {
                const quoted = self.lex.source[tok.span.start] == '"';
                const ident = try self.identifierFromSlice(name_slice, quoted, tok.span);
                try self.advance();
                return self.parseIdentifierExpression(ident);
            },
            .number => |lit| {
                try self.advance();
                return self.parseNumberLiteral(tok.span, lit);
            },
            .string => |raw| {
                try self.advance();
                return self.parseStringLiteral(tok.span, raw);
            },
            .keyword => |kw| switch (kw) {
                .True => {
                    try self.advance();
                    return self.makeLiteral(tok.span, .{ .boolean = true });
                },
                .False => {
                    try self.advance();
                    return self.makeLiteral(tok.span, .{ .boolean = false });
                },
                .Null => {
                    try self.advance();
                    return self.makeLiteral(tok.span, .null);
                },
                else => return ParseError.UnexpectedToken,
            },
            .symbol => |sym| switch (sym) {
                .l_paren => {
                    try self.advance();
                    const expr = try self.parseExpression(0);
                    try self.expectSymbol(.r_paren);
                    return expr;
                },
                .star => {
                    const star_span = tok.span;
                    try self.advance();
                    const expr_ptr = try self.arena.create(ast.Expression);
                    expr_ptr.* = .{ .star = .{ .qualifier = null, .span = star_span } };
                    return expr_ptr;
                },
                else => return ParseError.UnexpectedToken,
            },
            .eof => return ParseError.UnexpectedToken,
            .lex_error => return ParseError.LexError,
        }
    }

    fn parseIdentifierExpression(self: *Parser, ident: ast.Identifier) ParseError!*const ast.Expression {
        if (self.current.kind == .symbol and self.current.kind.symbol == .l_paren) {
            try self.advance();
            const args = try self.parseArgumentList();
            const span = spanFrom(ident.span.start, args.closing_span.end);
            const expr_ptr = try self.arena.create(ast.Expression);
            expr_ptr.* = .{ .function_call = .{
                .name = ident,
                .arguments = args.arguments,
                .distinct = args.distinct,
                .span = span,
            } };
            return expr_ptr;
        }

        var column_name = ident;
        var table_name: ?ast.Identifier = null;
        var span_start = ident.span.start;
        var span_end = ident.span.end;
        if (self.current.kind == .symbol and self.current.kind.symbol == .dot) {
            try self.advance();
            table_name = ident;
            span_start = ident.span.start;
            if (self.current.kind == .symbol and self.current.kind.symbol == .star) {
                const star_span = self.current.span;
                try self.advance();
                const expr_ptr = try self.arena.create(ast.Expression);
                expr_ptr.* = .{ .star = .{
                    .qualifier = table_name,
                    .span = spanFrom(span_start, star_span.end),
                } };
                return expr_ptr;
            }
            column_name = try self.expectIdentifier();
            span_end = column_name.span.end;
        }

        const expr_ptr = try self.arena.create(ast.Expression);
        expr_ptr.* = .{ .column = .{
            .table = table_name,
            .name = column_name,
            .span = spanFrom(span_start, span_end),
        } };
        return expr_ptr;
    }

    fn parseArgumentList(self: *Parser) ParseError!CallArguments {
        var distinct = false;
        if (self.current.kind == .keyword and self.current.kind.keyword == .Distinct) {
            try self.advance();
            distinct = true;
        }

        if (self.current.kind == .symbol and self.current.kind.symbol == .r_paren) {
            const close_span = self.current.span;
            try self.advance();
            if (distinct) return ParseError.ExpectedToken;
            return CallArguments{
                .arguments = &[_]*const ast.Expression{},
                .distinct = false,
                .closing_span = close_span,
            };
        }

        var args = TempList(*const ast.Expression).init(self.scratch);
        defer args.deinit();

        while (true) {
            const arg = try self.parseExpression(0);
            try args.append(arg);
            if (self.current.kind == .symbol and self.current.kind.symbol == .comma) {
                try self.advance();
            } else break;
        }
        if (!(self.current.kind == .symbol and self.current.kind.symbol == .r_paren)) {
            return ParseError.ExpectedToken;
        }
        const close_span = self.current.span;
        try self.advance();

        return CallArguments{
            .arguments = try self.copySlice(*const ast.Expression, args.items),
            .distinct = distinct,
            .closing_span = close_span,
        };
    }

    fn parseNumberLiteral(self: *Parser, span: lexmod.Span, lit: lexmod.NumberLiteral) ParseError!*const ast.Expression {
        switch (lit.kind) {
            .integer => {
                const value = std.fmt.parseInt(i64, lit.text, 10) catch return ParseError.InvalidNumber;
                return self.makeLiteral(span, .{ .integer = value });
            },
            .float => {
                const text = try self.internExact(lit.text);
                return self.makeLiteral(span, .{ .float = text });
            },
        }
    }

    fn parseStringLiteral(self: *Parser, span: lexmod.Span, raw: []const u8) ParseError!*const ast.Expression {
        const content = try self.unescapeSqlString(raw);
        return self.makeLiteral(span, .{ .string = content });
    }

    fn peekBinaryOperator(self: *Parser) ?BinaryOpInfo {
        const spec = operators.findBinaryOperator(self.current.kind) orelse return null;
        return BinaryOpInfo{ .op = spec.operator, .precedence = spec.precedence };
    }

    fn makeBinary(self: *Parser, op: ast.BinaryOperator, left: *const ast.Expression, right: *const ast.Expression) ParseError!*const ast.Expression {
        const expr_ptr = try self.arena.create(ast.Expression);
        const span = spanFrom(ast.expressionSpan(left).start, ast.expressionSpan(right).end);
        expr_ptr.* = .{ .binary = .{ .op = op, .left = left, .right = right, .span = span } };
        return expr_ptr;
    }

    fn makeUnary(self: *Parser, op: ast.UnaryOperator, span_start: usize, operand: *const ast.Expression) ParseError!*const ast.Expression {
        const expr_ptr = try self.arena.create(ast.Expression);
        const operand_span = ast.expressionSpan(operand);
        const span = spanFrom(span_start, operand_span.end);
        expr_ptr.* = .{ .unary = .{ .op = op, .operand = operand, .span = span } };
        return expr_ptr;
    }

    fn makeLiteral(self: *Parser, span: lexmod.Span, literal_value: ast.Literal.Value) ParseError!*const ast.Expression {
        const expr_ptr = try self.arena.create(ast.Expression);
        expr_ptr.* = .{ .literal = .{ .span = span, .value = literal_value } };
        return expr_ptr;
    }

    fn expectKeyword(self: *Parser, keyword: lexmod.Keyword) ParseError!void {
        switch (self.current.kind) {
            .keyword => |kw| {
                if (kw != keyword) return ParseError.ExpectedToken;
                try self.advance();
                return;
            },
            .lex_error => return ParseError.LexError,
            else => return ParseError.ExpectedToken,
        }
    }

    fn matchKeyword(self: *Parser, keyword: lexmod.Keyword) ParseError!bool {
        switch (self.current.kind) {
            .keyword => |kw| {
                if (kw != keyword) return false;
                try self.advance();
                return true;
            },
            .lex_error => return ParseError.LexError,
            else => return false,
        }
    }

    fn isKeyword(self: *Parser, keyword: lexmod.Keyword) bool {
        return self.current.kind == .keyword and self.current.kind.keyword == keyword;
    }

    fn expectSymbol(self: *Parser, sym: lexmod.Symbol) ParseError!void {
        switch (self.current.kind) {
            .symbol => |current_sym| {
                if (current_sym != sym) return ParseError.ExpectedToken;
                try self.advance();
                return;
            },
            .lex_error => return ParseError.LexError,
            else => return ParseError.ExpectedToken,
        }
    }

    fn matchSymbol(self: *Parser, sym: lexmod.Symbol) ParseError!bool {
        switch (self.current.kind) {
            .symbol => |current_sym| {
                if (current_sym != sym) return false;
                try self.advance();
                return true;
            },
            .lex_error => return ParseError.LexError,
            else => return false,
        }
    }

    fn consumeStatementTerminator(self: *Parser) ParseError!void {
        if (self.current.kind == .symbol and self.current.kind.symbol == .semicolon) {
            try self.advance();
        }
    }

    fn expectEof(self: *Parser) ParseError!void {
        switch (self.current.kind) {
            .eof => return,
            .lex_error => return ParseError.LexError,
            else => return ParseError.ExpectedToken,
        }
    }

    fn expectIdentifier(self: *Parser) ParseError!ast.Identifier {
        switch (self.current.kind) {
            .identifier => |slice| {
                const quoted = self.lex.source[self.current.span.start] == '"';
                const span = self.current.span;
                const ident = try self.identifierFromSlice(slice, quoted, span);
                try self.advance();
                return ident;
            },
            .lex_error => return ParseError.LexError,
            else => return ParseError.ExpectedToken,
        }
    }

    fn identifierFromSlice(self: *Parser, slice: []const u8, quoted: bool, span: lexmod.Span) ParseError!ast.Identifier {
        if (quoted) {
            const text = try self.unescapeQuotedIdentifier(slice);
            return ast.Identifier{
                .value = text,
                .canonical = text,
                .quoted = true,
                .span = span,
            };
        }

        const canonical = try self.internCanonicalLower(slice);
        return ast.Identifier{
            .value = slice,
            .canonical = canonical,
            .quoted = false,
            .span = span,
        };
    }

    /// Interns an exact byte slice ensuring the backing storage lives in the
    /// arena so references remain valid after scratch allocator resets.
    fn internExact(self: *Parser, bytes: []const u8) ParseError![]const u8 {
        if (bytes.len == 0) return &[_]u8{};
        const gop = try self.pool.getOrPut(self.scratch, bytes);
        if (!gop.found_existing) {
            const dest = try self.arena.alloc(u8, bytes.len);
            std.mem.copyForwards(u8, dest, bytes);
            gop.key_ptr.* = dest;
            gop.value_ptr.* = dest;
        }
        return gop.value_ptr.*;
    }

    fn internCanonicalLower(self: *Parser, bytes: []const u8) ParseError![]const u8 {
        if (bytes.len == 0) return &[_]u8{};
        var needs_lower = false;
        for (bytes) |ch| {
            if (std.ascii.toLower(ch) != ch) {
                needs_lower = true;
                break;
            }
        }

        if (!needs_lower) {
            const gop = try self.canonical_pool.getOrPut(self.scratch, bytes);
            if (!gop.found_existing) {
                gop.key_ptr.* = bytes;
                gop.value_ptr.* = bytes;
            }
            return gop.value_ptr.*;
        }

        const tmp = try self.scratch.alloc(u8, bytes.len);
        for (bytes, 0..) |ch, idx| {
            tmp[idx] = std.ascii.toLower(ch);
        }
        const gop = try self.canonical_pool.getOrPut(self.scratch, tmp);
        if (!gop.found_existing) {
            const dest = try self.arena.alloc(u8, tmp.len);
            std.mem.copyForwards(u8, dest, tmp);
            gop.key_ptr.* = dest;
            gop.value_ptr.* = dest;
        }
        return gop.value_ptr.*;
    }

    fn unescapeSqlString(self: *Parser, raw: []const u8) ParseError![]const u8 {
        if (raw.len == 0) return &[_]u8{};
        const escape_idx = std.mem.indexOf(u8, raw, "''");
        if (escape_idx == null) {
            return self.internExact(raw);
        }

        var buffer = try self.arena.alloc(u8, raw.len);
        var i: usize = 0;
        var out: usize = 0;
        while (i < raw.len) {
            if (raw[i] == '\'' and i + 1 < raw.len and raw[i + 1] == '\'') {
                buffer[out] = '\'';
                i += 2;
                out += 1;
                continue;
            }
            buffer[out] = raw[i];
            i += 1;
            out += 1;
        }
        return buffer[0..out];
    }

    fn unescapeQuotedIdentifier(self: *Parser, raw: []const u8) ParseError![]const u8 {
        if (raw.len == 0) return &[_]u8{};
        const escape_idx = std.mem.indexOf(u8, raw, "\"\"");
        if (escape_idx == null) {
            return self.internExact(raw);
        }

        var buffer = try self.arena.alloc(u8, raw.len);
        var i: usize = 0;
        var out: usize = 0;
        while (i < raw.len) {
            if (raw[i] == '"' and i + 1 < raw.len and raw[i + 1] == '"') {
                buffer[out] = '"';
                i += 2;
                out += 1;
                continue;
            }
            buffer[out] = raw[i];
            i += 1;
            out += 1;
        }

        const result = buffer[0..out];
        const gop = try self.pool.getOrPut(self.scratch, result);
        if (!gop.found_existing) {
            gop.key_ptr.* = result;
            gop.value_ptr.* = result;
        }
        return gop.value_ptr.*;
    }

    fn advance(self: *Parser) ParseError!void {
        self.current = self.lex.next();
        if (self.current.kind == .lex_error) {
            return ParseError.LexError;
        }
    }
};

// Tests ---------------------------------------------------------------------

test "parse simple select" {
    var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_inst.deinit();
    const allocator = arena_inst.allocator();

    const stmt = try parseSelect(allocator, "SELECT message FROM logs");
    try std.testing.expectEqual(@as(usize, 1), stmt.projection.len);
    try std.testing.expect(stmt.selection == null);
    try std.testing.expectEqual(@as(usize, 1), stmt.from.len);
    try std.testing.expect(std.mem.eql(u8, stmt.from[0].relation.table.name.value, "logs"));
}

test "parse quoted identifier unescapes" {
    var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_inst.deinit();
    const allocator = arena_inst.allocator();

    const stmt = try parseSelect(allocator, "SELECT \"A\"\"B\" FROM \"T\"\"able\"");

    const item = stmt.projection[0];
    const column_expr = switch (item.kind) {
        .expression => |expr_ptr| expr_ptr,
        else => unreachable,
    };
    const column = switch (column_expr.*) {
        .column => |col| col,
        else => unreachable,
    };
    try std.testing.expect(column.table == null);
    try std.testing.expect(std.mem.eql(u8, column.name.value, "A\"B"));

    const table_ref = switch (stmt.from[0].relation) {
        .table => |table| table,
        else => unreachable,
    };
    try std.testing.expect(std.mem.eql(u8, table_ref.name.value, "T\"able"));
}

test "parse reports lexical errors" {
    var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_inst.deinit();
    const allocator = arena_inst.allocator();

    try std.testing.expectError(ParseError.LexError, parseSelect(allocator, "SELECT ! FROM t"));
}

test "parse function call with where" {
    var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_inst.deinit();
    const allocator = arena_inst.allocator();

    const stmt = try parseSelect(allocator, "SELECT ABS(value) AS abs_value FROM metrics WHERE value >= 0");
    try std.testing.expectEqual(@as(usize, 1), stmt.projection.len);
    const item = stmt.projection[0];
    try std.testing.expect(item.alias != null);
    try std.testing.expect(stmt.selection != null);
}

test "parse select star" {
    var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_inst.deinit();
    const allocator = arena_inst.allocator();

    const stmt = try parseSelect(allocator, "SELECT * FROM my_table WHERE 1 = 1");
    try std.testing.expectEqual(@as(usize, 1), stmt.projection.len);
    try std.testing.expect(stmt.projection[0].kind == .star);
    try std.testing.expect(stmt.selection != null);
}

test "unary operators respect precedence" {
    var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_inst.deinit();
    const allocator = arena_inst.allocator();

    const stmt = try parseSelect(allocator, "SELECT -a * b, a * -b FROM t");

    const first_expr = switch (stmt.projection[0].kind) {
        .expression => |expr| expr,
        else => unreachable,
    };
    const first_bin = switch (first_expr.*) {
        .binary => |bin| bin,
        else => unreachable,
    };
    try std.testing.expectEqual(ast.BinaryOperator.multiply, first_bin.op);
    const first_left = switch (first_bin.left.*) {
        .unary => |un| un,
        else => unreachable,
    };
    try std.testing.expectEqual(ast.UnaryOperator.minus, first_left.op);
    const first_right = switch (first_bin.right.*) {
        .column => |col| col,
        else => unreachable,
    };
    const first_operand = switch (first_left.operand.*) {
        .column => |col| col,
        else => unreachable,
    };
    try std.testing.expect(std.mem.eql(u8, first_operand.name.value, "a"));
    try std.testing.expect(std.mem.eql(u8, first_right.name.value, "b"));

    const second_expr = switch (stmt.projection[1].kind) {
        .expression => |expr| expr,
        else => unreachable,
    };
    const second_bin = switch (second_expr.*) {
        .binary => |bin| bin,
        else => unreachable,
    };
    try std.testing.expectEqual(ast.BinaryOperator.multiply, second_bin.op);
    const second_left = switch (second_bin.left.*) {
        .column => |col| col,
        else => unreachable,
    };
    const second_right = switch (second_bin.right.*) {
        .unary => |un| un,
        else => unreachable,
    };
    try std.testing.expectEqual(ast.UnaryOperator.minus, second_right.op);
    const second_operand = switch (second_right.operand.*) {
        .column => |col| col,
        else => unreachable,
    };
    try std.testing.expect(std.mem.eql(u8, second_left.name.value, "a"));
    try std.testing.expect(std.mem.eql(u8, second_operand.name.value, "b"));
}

test "parse cte with join and using" {
    var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_inst.deinit();
    const allocator = arena_inst.allocator();

    const sql = "WITH derived AS (" ++
        "SELECT MAX(a) AS max_a, COUNT(b) AS b_num, user_id FROM MY_TABLE GROUP BY user_id" ++
        ") SELECT * FROM my_table LEFT JOIN derived USING (user_id)";

    const stmt = try parseSelect(allocator, sql);

    try std.testing.expect(stmt.with_clause != null);
    const with_clause = stmt.with_clause.?;
    try std.testing.expectEqual(@as(usize, 1), with_clause.ctes.len);
    try std.testing.expect(std.mem.eql(u8, with_clause.ctes[0].name.value, "derived"));

    try std.testing.expectEqual(@as(usize, 1), stmt.projection.len);
    try std.testing.expect(stmt.projection[0].kind == .star);

    try std.testing.expectEqual(@as(usize, 1), stmt.from.len);
    const from_item = stmt.from[0];
    try std.testing.expectEqual(@as(usize, 1), from_item.joins.len);
    try std.testing.expect(from_item.joins[0].kind == .left);
    const using_constraint = from_item.joins[0].constraint;
    try std.testing.expect(using_constraint == .using);
    try std.testing.expectEqual(@as(usize, 1), using_constraint.using.len);
    try std.testing.expect(std.mem.eql(u8, using_constraint.using[0].value, "user_id"));

    const cte_query = with_clause.ctes[0].query;
    try std.testing.expect(cte_query.group_by.len > 0);
}

test "parse order by and having" {
    var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_inst.deinit();
    const allocator = arena_inst.allocator();

    const sql = "SELECT user_id, COUNT(*) AS total FROM events GROUP BY user_id HAVING COUNT(*) > 10 ORDER BY total DESC";
    const stmt = try parseSelect(allocator, sql);

    try std.testing.expectEqual(@as(usize, 2), stmt.projection.len);
    try std.testing.expectEqual(@as(usize, 1), stmt.group_by.len);
    try std.testing.expect(stmt.having != null);
    try std.testing.expectEqual(@as(usize, 1), stmt.order_by.len);
    try std.testing.expect(stmt.order_by[0].direction == .desc);
}

test "parse boolean and null literals" {
    var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_inst.deinit();
    const allocator = arena_inst.allocator();

    const stmt = try parseSelect(allocator, "SELECT TRUE, FALSE, NULL FROM logs");

    const first_literal_expr = switch (stmt.projection[0].kind) {
        .expression => |expr_ptr| expr_ptr,
        else => unreachable,
    };
    const second_literal_expr = switch (stmt.projection[1].kind) {
        .expression => |expr_ptr| expr_ptr,
        else => unreachable,
    };
    const third_literal_expr = switch (stmt.projection[2].kind) {
        .expression => |expr_ptr| expr_ptr,
        else => unreachable,
    };

    const first_literal = switch (first_literal_expr.*) {
        .literal => |lit| lit,
        else => unreachable,
    };
    const second_literal = switch (second_literal_expr.*) {
        .literal => |lit| lit,
        else => unreachable,
    };
    const third_literal = switch (third_literal_expr.*) {
        .literal => |lit| lit,
        else => unreachable,
    };

    const first_bool = switch (first_literal.value) {
        .boolean => |value| value,
        else => unreachable,
    };
    try std.testing.expect(first_bool);
    switch (second_literal.value) {
        .boolean => |value| try std.testing.expect(!value),
        else => unreachable,
    }
    switch (third_literal.value) {
        .null => {},
        else => unreachable,
    }
}

test "spans cover select components" {
    var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_inst.deinit();
    const allocator = arena_inst.allocator();

    const query = "SELECT value + 1 AS next_value FROM logs";
    const stmt = try parseSelect(allocator, query);

    try std.testing.expectEqual(@as(usize, 0), stmt.span.start);
    try std.testing.expectEqual(@as(usize, query.len), stmt.span.end);

    try std.testing.expectEqual(@as(usize, 1), stmt.projection.len);
    const select_item = stmt.projection[0];

    const value_start = std.mem.indexOf(u8, query, "value") orelse unreachable;
    const alias_start = std.mem.indexOf(u8, query, "next_value") orelse unreachable;
    try std.testing.expectEqual(value_start, select_item.span.start);
    try std.testing.expectEqual(alias_start + "next_value".len, select_item.span.end);

    const expr = switch (select_item.kind) {
        .expression => |expr_ptr| expr_ptr,
        .star => unreachable,
    };
    const expr_span = ast.expressionSpan(expr);
    const literal_start = std.mem.indexOf(u8, query, "1 AS") orelse unreachable;
    try std.testing.expectEqual(value_start, expr_span.start);
    try std.testing.expectEqual(literal_start + 1, expr_span.end);

    const binary = switch (expr.*) {
        .binary => |bin| bin,
        else => unreachable,
    };
    const literal_span = ast.expressionSpan(binary.right);
    try std.testing.expectEqual(literal_start, literal_span.start);
    try std.testing.expectEqual(literal_start + 1, literal_span.end);

    try std.testing.expectEqual(@as(usize, 1), stmt.from.len);
    const table_expr = stmt.from[0];
    const table_name_pos = std.mem.indexOf(u8, query, "logs") orelse unreachable;
    try std.testing.expectEqual(table_name_pos, table_expr.span.start);
    try std.testing.expectEqual(table_name_pos + "logs".len, table_expr.span.end);
}

test "parse select distinct" {
    var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_inst.deinit();
    const allocator = arena_inst.allocator();

    const stmt = try parseSelect(allocator, "SELECT DISTINCT message FROM logs");
    try std.testing.expect(stmt.distinct);
    try std.testing.expectEqual(@as(usize, 1), stmt.projection.len);
}

test "parse function call distinct" {
    var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_inst.deinit();
    const allocator = arena_inst.allocator();

    const stmt = try parseSelect(allocator, "SELECT COUNT(DISTINCT user_id) FROM events");
    const item = stmt.projection[0];
    const expr_ptr = switch (item.kind) {
        .expression => |expr| expr,
        else => unreachable,
    };
    const call = switch (expr_ptr.*) {
        .function_call => |call| call,
        else => unreachable,
    };

    try std.testing.expect(call.distinct);
    try std.testing.expectEqual(@as(usize, 1), call.arguments.len);
    const arg_expr = call.arguments[0];
    const column = switch (arg_expr.*) {
        .column => |col| col,
        else => unreachable,
    };
    try std.testing.expect(std.mem.eql(u8, column.name.canonical, "user_id"));
}

test "identifiers reuse canonical forms" {
    var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_inst.deinit();
    const allocator = arena_inst.allocator();

    const stmt = try parseSelect(allocator, "SELECT Foo, FOO FROM Foo");

    const first_item = stmt.projection[0];
    const second_item = stmt.projection[1];
    const first_expr = switch (first_item.kind) {
        .expression => |expr_ptr| expr_ptr,
        else => unreachable,
    };
    const second_expr = switch (second_item.kind) {
        .expression => |expr_ptr| expr_ptr,
        else => unreachable,
    };
    const first_col = switch (first_expr.*) {
        .column => |col| col,
        else => unreachable,
    };
    const second_col = switch (second_expr.*) {
        .column => |col| col,
        else => unreachable,
    };

    try std.testing.expect(std.mem.eql(u8, first_col.name.value, "Foo"));
    try std.testing.expect(std.mem.eql(u8, second_col.name.value, "FOO"));
    try std.testing.expect(first_col.name.canonical.ptr == second_col.name.canonical.ptr);

    const table = switch (stmt.from[0].relation) {
        .table => |t| t,
        else => unreachable,
    };
    try std.testing.expect(std.mem.eql(u8, table.name.value, "Foo"));
    try std.testing.expect(table.name.canonical.ptr == first_col.name.canonical.ptr);
}

test "parse with reusable scratch" {
    var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_inst.deinit();
    const allocator = arena_inst.allocator();

    var scratch = Scratch.init(std.heap.page_allocator);
    defer scratch.deinit();

    const first = try parseSelectWithScratch(allocator, &scratch, "SELECT message FROM logs");
    try std.testing.expectEqual(@as(usize, 1), first.projection.len);

    const second = try parseSelectWithScratch(allocator, &scratch, "SELECT COUNT(*) FROM metrics");
    try std.testing.expectEqual(@as(usize, 1), second.projection.len);
}

test "parse select with optional semicolon" {
    var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_inst.deinit();
    const allocator = arena_inst.allocator();

    _ = try parseSelect(allocator, "SELECT 1;");
    _ = try parseSelect(allocator, "SELECT 1");
}

test "parse select rejects double semicolon" {
    var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_inst.deinit();
    const allocator = arena_inst.allocator();

    try std.testing.expectError(ParseError.ExpectedToken, parseSelect(allocator, "SELECT 1;;"));
}

test "parse select rejects trailing junk" {
    var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_inst.deinit();
    const allocator = arena_inst.allocator();

    try std.testing.expectError(ParseError.ExpectedToken, parseSelect(allocator, "SELECT 1 )"));
}

test "parse select requires comma between expressions" {
    var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_inst.deinit();
    const allocator = arena_inst.allocator();

    try std.testing.expectError(ParseError.ExpectedToken, parseSelect(allocator, "SELECT a b, c FROM tbl"));
}

test "parse derived table without alias is error" {
    var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_inst.deinit();
    const allocator = arena_inst.allocator();

    try std.testing.expectError(ParseError.ExpectedToken, parseSelect(allocator, "SELECT * FROM (SELECT 1)"));
}

test "parse subtraction is left associative" {
    var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_inst.deinit();
    const allocator = arena_inst.allocator();

    const stmt = try parseSelect(allocator, "SELECT a - b - c FROM nums");
    const item = stmt.projection[0];
    const expr_ptr = switch (item.kind) {
        .expression => |expr| expr,
        else => unreachable,
    };
    const outer = switch (expr_ptr.*) {
        .binary => |bin| bin,
        else => unreachable,
    };
    try std.testing.expect(outer.op == .subtract);
    const left_expr = switch (outer.left.*) {
        .binary => |bin| bin,
        else => unreachable,
    };
    try std.testing.expect(left_expr.op == .subtract);
    const right_name = switch (outer.right.*) {
        .column => |col| col.name.canonical,
        else => unreachable,
    };
    try std.testing.expect(std.mem.eql(u8, right_name, "c"));
}

test "parse logical precedence matches expectations" {
    var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_inst.deinit();
    const allocator = arena_inst.allocator();

    const stmt = try parseSelect(allocator, "SELECT a AND b OR c FROM events");
    const expr_ptr = switch (stmt.projection[0].kind) {
        .expression => |expr| expr,
        else => unreachable,
    };
    const outer = switch (expr_ptr.*) {
        .binary => |bin| bin,
        else => unreachable,
    };
    try std.testing.expect(outer.op == .logical_or);
    const left = switch (outer.left.*) {
        .binary => |bin| bin,
        else => unreachable,
    };
    try std.testing.expect(left.op == .logical_and);
}

test "parse unary precedence binds tighter than and" {
    var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_inst.deinit();
    const allocator = arena_inst.allocator();

    const stmt = try parseSelect(allocator, "SELECT NOT a AND b FROM logs");
    const expr_ptr = switch (stmt.projection[0].kind) {
        .expression => |expr| expr,
        else => unreachable,
    };
    const outer = switch (expr_ptr.*) {
        .binary => |bin| bin,
        else => unreachable,
    };
    try std.testing.expect(outer.op == .logical_and);
    const left = switch (outer.left.*) {
        .unary => |unary| unary,
        else => unreachable,
    };
    try std.testing.expect(left.op == .not);
}

test "parse multiple joins preserve order" {
    var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_inst.deinit();
    const allocator = arena_inst.allocator();

    const sql = "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id LEFT JOIN t3 ON t2.id = t3.id";
    const stmt = try parseSelect(allocator, sql);
    try std.testing.expectEqual(@as(usize, 1), stmt.from.len);
    const from_item = stmt.from[0];
    try std.testing.expectEqual(@as(usize, 2), from_item.joins.len);
    try std.testing.expect(from_item.joins[0].kind == .inner);
    try std.testing.expect(from_item.joins[1].kind == .left);
}
