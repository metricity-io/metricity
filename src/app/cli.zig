const std = @import("std");

pub const ParseError = error{
    UnknownCommand,
    MissingCommand,
    MissingOptionValue,
    MissingConfigPath,
    UnexpectedArgument,
    InvalidNumber,
};

pub const Command = enum {
    help,
    run,
    check,
};

pub const RunOptions = struct {
    config_path: []const u8,
    max_batches: ?usize = null,
    once: bool = false,
};

pub const CheckOptions = struct {
    config_path: []const u8,
};

pub const Parsed = union(Command) {
    help: void,
    run: RunOptions,
    check: CheckOptions,
};

pub fn parse(args: []const []const u8) ParseError!Parsed {
    if (args.len <= 1) return Parsed{ .help = {} };

    var index: usize = 1;
    const token = args[index];
    index += 1;

    if (std.mem.eql(u8, token, "--help") or std.mem.eql(u8, token, "-h")) {
        return Parsed{ .help = {} };
    }

    if (std.mem.eql(u8, token, "help")) {
        return Parsed{ .help = {} };
    }

    if (std.mem.eql(u8, token, "run")) {
        return parseRun(args, &index);
    }

    if (std.mem.eql(u8, token, "check")) {
        return parseCheck(args, &index);
    }

    return ParseError.UnknownCommand;
}

fn parseRun(args: []const []const u8, index_ptr: *usize) ParseError!Parsed {
    var config_path: ?[]const u8 = null;
    var max_batches: ?usize = null;
    var once = false;
    var index = index_ptr.*;

    while (index < args.len) {
        const token = args[index];
        index += 1;

        if (std.mem.eql(u8, token, "--config")) {
            const value = try consumeValue(args, &index);
            config_path = value;
            continue;
        }

        if (std.mem.eql(u8, token, "--max-batches")) {
            const value = try consumeValue(args, &index);
            const parsed = std.fmt.parseInt(usize, value, 10) catch return ParseError.InvalidNumber;
            max_batches = parsed;
            continue;
        }

        if (std.mem.eql(u8, token, "--once")) {
            once = true;
            continue;
        }

        if (std.mem.eql(u8, token, "--help") or std.mem.eql(u8, token, "-h")) {
            return Parsed{ .help = {} };
        }

        if (std.mem.eql(u8, token, "--")) {
            // Nothing else is currently supported after `--`.
            break;
        }

        return ParseError.UnexpectedArgument;
    }

    if (config_path == null) return ParseError.MissingConfigPath;

    index_ptr.* = index;
    return Parsed{ .run = .{
        .config_path = config_path.?,
        .max_batches = max_batches,
        .once = once,
    } };
}

fn parseCheck(args: []const []const u8, index_ptr: *usize) ParseError!Parsed {
    var config_path: ?[]const u8 = null;
    var index = index_ptr.*;

    while (index < args.len) {
        const token = args[index];
        index += 1;

        if (std.mem.eql(u8, token, "--config")) {
            const value = try consumeValue(args, &index);
            config_path = value;
            continue;
        }

        if (std.mem.eql(u8, token, "--help") or std.mem.eql(u8, token, "-h")) {
            return Parsed{ .help = {} };
        }

        return ParseError.UnexpectedArgument;
    }

    if (config_path == null) return ParseError.MissingConfigPath;

    index_ptr.* = index;
    return Parsed{ .check = .{ .config_path = config_path.? } };
}

fn consumeValue(args: []const []const u8, index_ptr: *usize) ParseError![]const u8 {
    const index = index_ptr.*;
    if (index >= args.len) return ParseError.MissingOptionValue;
    const value = args[index];
    index_ptr.* = index + 1;
    return value;
}

pub fn usage() []const u8 {
    return (
        "Usage: metricity <command> [options]\n" ++
        "\n" ++
        "Commands:\n" ++
        "  run      Start the pipeline\n" ++
        "  check    Validate configuration and exit\n" ++
        "  help     Show this message\n" ++
        "\n" ++
        "Run options:\n" ++
        "  --config <path>         Path to configuration file (required)\n" ++
        "  --max-batches <count>   Process up to <count> batches and exit\n" ++
        "  --once                  Process a single batch and exit\n" ++
        "\n" ++
        "Check options:\n" ++
        "  --config <path>         Path to configuration file (required)\n" ++
        "\n" ++
        "Use `metricity <command> --help` for command-specific help.\n"
    );
}

const Expect = std.testing.expect;
const ExpectEqualStrings = std.testing.expectEqualStrings;

test "parse help by default" {
    var args = [_][]const u8{"metricity"};
    const parsed = try parse(args[0..]);
    try Expect(parsed == .help);
}

test "parse run with config" {
    var args = [_][]const u8{"metricity", "run", "--config", "config.toml"};
    const parsed = try parse(args[0..]);
    try Expect(parsed == .run);
    const run_opts = parsed.run;
    try ExpectEqualStrings("config.toml", run_opts.config_path);
    try Expect(run_opts.max_batches == null);
}

test "parse run with max batches" {
    var args = [_][]const u8{"metricity", "run", "--config", "cfg", "--max-batches", "3", "--once"};
    const parsed = try parse(args[0..]);
    try Expect(parsed == .run);
    const run_opts = parsed.run;
    try Expect(run_opts.max_batches.? == 3);
    try Expect(run_opts.once);
}

test "parse check command" {
    var args = [_][]const u8{"metricity", "check", "--config", "cfg"};
    const parsed = try parse(args[0..]);
    try Expect(parsed == .check);
    const check_opts = parsed.check;
    try ExpectEqualStrings("cfg", check_opts.config_path);
}
