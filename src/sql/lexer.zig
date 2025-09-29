const std = @import("std");

pub const Span = struct {
    start: usize,
    end: usize,
};

pub const Keyword = enum {
    Select,
    From,
    Where,
    As,
    And,
    Or,
    Not,
    With,
    Recursive,
    Join,
    Left,
    Inner,
    Outer,
    On,
    Using,
    Group,
    By,
    Having,
    Order,
    Asc,
    Desc,
    Distinct,
    True,
    False,
    Null,
};

pub const Symbol = enum {
    comma,
    dot,
    semicolon,
    star,
    plus,
    minus,
    slash,
    l_paren,
    r_paren,
    equal,
    not_equal,
    less,
    less_equal,
    greater,
    greater_equal,
};

pub const NumberKind = enum { integer, float };

pub const NumberLiteral = struct {
    kind: NumberKind,
    text: []const u8,
};

pub const ErrorKind = enum {
    invalid_character,
    unterminated_string,
    unterminated_quoted_identifier,
    unterminated_block_comment,
    invalid_number,
};

pub const Error = struct {
    kind: ErrorKind,
    fragment: []const u8,
};

pub const Token = struct {
    kind: Kind,
    span: Span,

    pub const Kind = union(enum) {
        eof,
        identifier: []const u8,
        keyword: Keyword,
        number: NumberLiteral,
        string: []const u8,
        symbol: Symbol,
        lex_error: Error,
    };
};

pub const Lexer = struct {
    source: []const u8,
    index: usize = 0,

    pub fn init(source: []const u8) Lexer {
        return .{ .source = source };
    }

    pub fn next(self: *Lexer) Token {
        if (self.skipWhitespace()) |token| {
            return token;
        }
        const start = self.index;

        if (self.index >= self.source.len) {
            return .{ .kind = .eof, .span = .{ .start = self.index, .end = self.index } };
        }

        const c = self.source[self.index];

        if (std.ascii.isAlphabetic(c) or c == '_' or c == '$') {
            const end = self.consumeIdentifier();
            const slice = self.source[start..end];
            const keyword = keywordFromSlice(slice);
            if (keyword) |kw| {
                return .{ .kind = .{ .keyword = kw }, .span = .{ .start = start, .end = end } };
            }
            return .{ .kind = .{ .identifier = slice }, .span = .{ .start = start, .end = end } };
        }

        if (std.ascii.isDigit(c) or (c == '.' and self.index + 1 < self.source.len and std.ascii.isDigit(self.source[self.index + 1]))) {
            return self.consumeNumber(start);
        }

        switch (c) {
            ',' => {
                self.index += 1;
                return tokenSymbol(.comma, start, self.index);
            },
            ';' => {
                self.index += 1;
                return tokenSymbol(.semicolon, start, self.index);
            },
            '.' => {
                self.index += 1;
                return tokenSymbol(.dot, start, self.index);
            },
            '*' => {
                self.index += 1;
                return tokenSymbol(.star, start, self.index);
            },
            '+' => {
                self.index += 1;
                return tokenSymbol(.plus, start, self.index);
            },
            '-' => {
                self.index += 1;
                return tokenSymbol(.minus, start, self.index);
            },
            '/' => {
                self.index += 1;
                return tokenSymbol(.slash, start, self.index);
            },
            '(' => {
                self.index += 1;
                return tokenSymbol(.l_paren, start, self.index);
            },
            ')' => {
                self.index += 1;
                return tokenSymbol(.r_paren, start, self.index);
            },
            '=' => {
                self.index += 1;
                return tokenSymbol(.equal, start, self.index);
            },
            '<' => {
                self.index += 1;
                if (self.matchChar('=')) {
                    return tokenSymbol(.less_equal, start, self.index);
                }
                if (self.matchChar('>')) {
                    return tokenSymbol(.not_equal, start, self.index);
                }
                return tokenSymbol(.less, start, self.index);
            },
            '>' => {
                self.index += 1;
                if (self.matchChar('=')) {
                    return tokenSymbol(.greater_equal, start, self.index);
                }
                return tokenSymbol(.greater, start, self.index);
            },
            '!' => {
                self.index += 1;
                if (self.matchChar('=')) {
                    return tokenSymbol(.not_equal, start, self.index);
                }
                return makeErrorToken(self, .invalid_character, start, self.index);
            },
            '\'' => {
                return self.consumeString(start);
            },
            '"' => {
                return self.consumeQuotedIdentifier(start);
            },
            else => {}
        }

        return makeErrorToken(self, .invalid_character, start, start + 1);
    }

    fn skipWhitespace(self: *Lexer) ?Token {
        while (self.index < self.source.len) {
            const c = self.source[self.index];
            switch (c) {
                ' ', '\n', '\r', '\t' => self.index += 1,
                '-' => {
                    if (self.matchAhead("--")) {
                        self.index += 2;
                        while (self.index < self.source.len and self.source[self.index] != '\n') {
                            self.index += 1;
                        }
                    } else {
                        return null;
                    }
                },
                '/' => {
                    if (self.matchAhead("/*")) {
                        const comment_start = self.index;
                        self.index += 2;
                        var closed = false;
                        while (self.index < self.source.len) {
                            if (self.source[self.index] == '*' and self.index + 1 < self.source.len and self.source[self.index + 1] == '/') {
                                self.index += 2;
                                closed = true;
                                break;
                            }
                            self.index += 1;
                        }
                        if (!closed) {
                            return makeErrorToken(self, .unterminated_block_comment, comment_start, self.source.len);
                        }
                        continue;
                    } else {
                        return null;
                    }
                },
                else => return null,
            }
        }
        return null;
    }

    fn matchAhead(self: Lexer, literal: []const u8) bool {
        if (self.index + literal.len > self.source.len) return false;
        return std.mem.eql(u8, self.source[self.index .. self.index + literal.len], literal);
    }

    fn matchChar(self: *Lexer, ch: u8) bool {
        if (self.index < self.source.len and self.source[self.index] == ch) {
            self.index += 1;
            return true;
        }
        return false;
    }

    fn consumeIdentifier(self: *Lexer) usize {
        while (self.index < self.source.len) : (self.index += 1) {
            const c = self.source[self.index];
            if (!(std.ascii.isAlphabetic(c) or std.ascii.isDigit(c) or c == '_' or c == '$')) break;
        }
        return self.index;
    }

    fn consumeNumber(self: *Lexer, start: usize) Token {
        var kind: NumberKind = .integer;
        const len = self.source.len;

        var digits_before_dot = false;
        while (self.index < len and std.ascii.isDigit(self.source[self.index])) {
            self.index += 1;
            digits_before_dot = true;
        }

        var digits_after_dot = false;
        if (self.index < len and self.source[self.index] == '.') {
            kind = .float;
            self.index += 1;
            while (self.index < len and std.ascii.isDigit(self.source[self.index])) {
                self.index += 1;
                digits_after_dot = true;
            }

            if (!digits_before_dot and !digits_after_dot) {
                return makeErrorToken(self, .invalid_number, start, self.index);
            }
        }

        if (self.index < len and (self.source[self.index] == 'e' or self.source[self.index] == 'E')) {
            kind = .float;
            self.index += 1;
            _ = self.matchChar('+') or self.matchChar('-');
            const digits_start = self.index;
            while (self.index < len and std.ascii.isDigit(self.source[self.index])) {
                self.index += 1;
            }
            if (self.index == digits_start) {
                return makeErrorToken(self, .invalid_number, start, self.index);
            }
        }

        const end = self.index;
        return .{ .kind = .{ .number = .{ .kind = kind, .text = self.source[start..end] } }, .span = .{ .start = start, .end = end } };
    }

    fn consumeString(self: *Lexer, start: usize) Token {
        self.index += 1; // skip opening quote
        const content_start = self.index;
        while (self.index < self.source.len) {
            const c = self.source[self.index];
            if (c == '\'') {
                if (self.index + 1 < self.source.len and self.source[self.index + 1] == '\'') {
                    self.index += 2;
                    continue;
                }
                const end_quote = self.index;
                self.index += 1;
                return .{ .kind = .{ .string = self.source[content_start..end_quote] }, .span = .{ .start = start, .end = self.index } };
            }
            self.index += 1;
        }
        return makeErrorToken(self, .unterminated_string, start, self.index);
    }

    fn consumeQuotedIdentifier(self: *Lexer, start: usize) Token {
        self.index += 1; // skip opening double quote
        const content_start = self.index;
        while (self.index < self.source.len) {
            const c = self.source[self.index];
            if (c == '"') {
                if (self.index + 1 < self.source.len and self.source[self.index + 1] == '"') {
                    self.index += 2;
                    continue;
                }
                const end_quote = self.index;
                self.index += 1;
                return .{ .kind = .{ .identifier = self.source[content_start..end_quote] }, .span = .{ .start = start, .end = self.index } };
            }
            self.index += 1;
        }
        return makeErrorToken(self, .unterminated_quoted_identifier, start, self.index);
    }
};

fn tokenSymbol(sym: Symbol, start: usize, end: usize) Token {
    return .{ .kind = .{ .symbol = sym }, .span = .{ .start = start, .end = end } };
}

fn clampEnd(source_len: usize, end: usize) usize {
    return if (end > source_len) source_len else end;
}

fn makeFragment(source: []const u8, start: usize, end: usize) []const u8 {
    const clamped_end = clampEnd(source.len, end);
    return source[start..clamped_end];
}

fn asciiUpper(c: u8) u8 {
    return if (c >= 'a' and c <= 'z') c - 32 else c;
}

inline fn matchKeyword(slice: []const u8, comptime upper: []const u8) bool {
    if (slice.len != upper.len) return false;
    for (slice, 0..) |byte, idx| {
        if (asciiUpper(byte) != upper[idx]) return false;
    }
    return true;
}

fn keywordFromSlice(slice: []const u8) ?Keyword {
    if (slice.len == 0) return null;

    const first = asciiUpper(slice[0]);
    switch (slice.len) {
        2 => switch (first) {
            'A' => if (matchKeyword(slice, "AS")) return .As,
            'B' => if (matchKeyword(slice, "BY")) return .By,
            'O' => {
                if (matchKeyword(slice, "ON")) return .On;
                if (matchKeyword(slice, "OR")) return .Or;
            },
            else => {},
        },
        3 => switch (first) {
            'A' => {
                if (matchKeyword(slice, "AND")) return .And;
                if (matchKeyword(slice, "ASC")) return .Asc;
            },
            'N' => if (matchKeyword(slice, "NOT")) return .Not,
            else => {},
        },
        4 => switch (first) {
            'D' => if (matchKeyword(slice, "DESC")) return .Desc,
            'F' => if (matchKeyword(slice, "FROM")) return .From,
            'J' => if (matchKeyword(slice, "JOIN")) return .Join,
            'L' => if (matchKeyword(slice, "LEFT")) return .Left,
            'N' => if (matchKeyword(slice, "NULL")) return .Null,
            'T' => if (matchKeyword(slice, "TRUE")) return .True,
            'W' => if (matchKeyword(slice, "WITH")) return .With,
            else => {},
        },
        5 => switch (first) {
            'F' => if (matchKeyword(slice, "FALSE")) return .False,
            'G' => if (matchKeyword(slice, "GROUP")) return .Group,
            'I' => if (matchKeyword(slice, "INNER")) return .Inner,
            'O' => {
                if (matchKeyword(slice, "ORDER")) return .Order;
                if (matchKeyword(slice, "OUTER")) return .Outer;
            },
            'U' => if (matchKeyword(slice, "USING")) return .Using,
            'W' => if (matchKeyword(slice, "WHERE")) return .Where,
            else => {},
        },
        6 => switch (first) {
            'H' => if (matchKeyword(slice, "HAVING")) return .Having,
            'S' => if (matchKeyword(slice, "SELECT")) return .Select,
            else => {},
        },
        8 => if (matchKeyword(slice, "DISTINCT")) return .Distinct,
        9 => if (matchKeyword(slice, "RECURSIVE")) return .Recursive,
        else => {},
    }
    return null;
}

fn makeErrorToken(lexer: *Lexer, kind: ErrorKind, start: usize, end: usize) Token {
    const fragment = makeFragment(lexer.source, start, end);
    return .{
        .kind = .{ .lex_error = .{ .kind = kind, .fragment = fragment } },
        .span = .{ .start = start, .end = start + fragment.len },
    };
}

test "lex simple select" {
    const query = "SELECT message FROM logs WHERE level = 'info'";
    var lexer = Lexer.init(query);

    const expected = [_]Token.Kind{
        .{ .keyword = .Select },
        .{ .identifier = "message" },
        .{ .keyword = .From },
        .{ .identifier = "logs" },
        .{ .keyword = .Where },
        .{ .identifier = "level" },
        .{ .symbol = .equal },
        .{ .string = "info" },
        .eof,
    };

    var idx: usize = 0;
    while (true) : (idx += 1) {
        const tok = lexer.next();
        try std.testing.expect(idx < expected.len);
        try expectTokenEqual(expected[idx], tok.kind);
        if (tok.kind == .eof) break;
    }
}

test "lex numbers and arithmetic" {
    const query = "SELECT value + 42.5 FROM metrics WHERE value >= 10";
    var lexer = Lexer.init(query);

    const expected_tags = [_]Token.Kind{
        .{ .keyword = .Select },
        .{ .identifier = "value" },
        .{ .symbol = .plus },
        .{ .number = .{ .kind = .float, .text = "42.5" } },
        .{ .keyword = .From },
        .{ .identifier = "metrics" },
        .{ .keyword = .Where },
        .{ .identifier = "value" },
        .{ .symbol = .greater_equal },
        .{ .number = .{ .kind = .integer, .text = "10" } },
        .eof,
    };

    for (expected_tags) |expected_kind| {
        const tok = lexer.next();
        try expectTokenEqual(expected_kind, tok.kind);
    }
}

test "lex block comments and not equal" {
    const query = "/* head */ SeLeCt a != b FrOm t -- tail\n";
    var lexer = Lexer.init(query);

    const expected = [_]Token.Kind{
        .{ .keyword = .Select },
        .{ .identifier = "a" },
        .{ .symbol = .not_equal },
        .{ .identifier = "b" },
        .{ .keyword = .From },
        .{ .identifier = "t" },
        .eof,
    };

    for (expected) |expected_kind| {
        const tok = lexer.next();
        try expectTokenEqual(expected_kind, tok.kind);
    }
}

test "lex quoted identifier escapes" {
    const query = "SELECT \"weird\"\"name\" FROM \"T\"\"able\"";
    var lexer = Lexer.init(query);

    const expected = [_]Token.Kind{
        .{ .keyword = .Select },
        .{ .identifier = "weird\"\"name" },
        .{ .keyword = .From },
        .{ .identifier = "T\"\"able" },
        .eof,
    };

    for (expected) |expected_kind| {
        const tok = lexer.next();
        try expectTokenEqual(expected_kind, tok.kind);
    }
}

test "lex leading dot float" {
    const query = ".5";
    var lexer = Lexer.init(query);

    const number_tok = lexer.next();
    try expectTokenEqual(.{ .number = .{ .kind = .float, .text = ".5" } }, number_tok.kind);
    const eof_tok = lexer.next();
    try expectTokenEqual(.eof, eof_tok.kind);
}

test "lex invalid exponent produces error" {
    const query = "1e+";
    var lexer = Lexer.init(query);

    const err_tok = lexer.next();
    try expectTokenEqual(.{ .lex_error = .{ .kind = .invalid_number, .fragment = "1e+" } }, err_tok.kind);
}

test "lex unterminated block comment" {
    const query = "/* open";
    var lexer = Lexer.init(query);
    const err_tok = lexer.next();
    try expectTokenEqual(.{ .lex_error = .{ .kind = .unterminated_block_comment, .fragment = "/* open" } }, err_tok.kind);
}

test "lex lone bang is error" {
    const query = "!";
    var lexer = Lexer.init(query);
    const err_tok = lexer.next();
    try expectTokenEqual(.{ .lex_error = .{ .kind = .invalid_character, .fragment = "!" } }, err_tok.kind);
}

test "lex distinct and booleans" {
    const query = "SELECT DISTINCT TRUE, FALSE, NULL FROM t";
    var lexer = Lexer.init(query);

    const expected = [_]Token.Kind{
        .{ .keyword = .Select },
        .{ .keyword = .Distinct },
        .{ .keyword = .True },
        .{ .symbol = .comma },
        .{ .keyword = .False },
        .{ .symbol = .comma },
        .{ .keyword = .Null },
        .{ .keyword = .From },
        .{ .identifier = "t" },
        .eof,
    };

    for (expected) |expected_kind| {
        const tok = lexer.next();
        try expectTokenEqual(expected_kind, tok.kind);
    }
}

test "lex trailing semicolon" {
    const query = "SELECT 1;";
    var lexer = Lexer.init(query);

    const expected = [_]Token.Kind{
        .{ .keyword = .Select },
        .{ .number = .{ .kind = .integer, .text = "1" } },
        .{ .symbol = .semicolon },
        .eof,
    };

    for (expected) |expected_kind| {
        const tok = lexer.next();
        try expectTokenEqual(expected_kind, tok.kind);
    }
}

fn expectTokenEqual(expected: Token.Kind, actual: Token.Kind) !void {
    try std.testing.expect(std.meta.activeTag(expected) == std.meta.activeTag(actual));

    switch (expected) {
        .identifier => |value| try std.testing.expect(std.mem.eql(u8, value, actual.identifier)),
        .keyword => |kw| try std.testing.expectEqual(kw, actual.keyword),
        .number => |num| {
            try std.testing.expectEqual(num.kind, actual.number.kind);
            try std.testing.expect(std.mem.eql(u8, num.text, actual.number.text));
        },
        .string => |value| try std.testing.expect(std.mem.eql(u8, value, actual.string)),
        .symbol => |sym| try std.testing.expectEqual(sym, actual.symbol),
        .eof => {},
        .lex_error => |err| {
            try std.testing.expectEqual(err.kind, actual.lex_error.kind);
            try std.testing.expect(std.mem.eql(u8, err.fragment, actual.lex_error.fragment));
        },
    }
}
