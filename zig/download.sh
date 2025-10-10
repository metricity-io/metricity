#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
cd "$SCRIPT_DIR"

ZIG_RELEASE="0.15.1"
ZIG_BASE_URL="https://ziglang.org/download"
ZIG_CHECKSUMS=$(cat <<'EOF'
https://ziglang.org/download/0.15.1/zig-aarch64-linux-0.15.1.tar.xz bb4a8d2ad735e7fba764c497ddf4243cb129fece4148da3222a7046d3f1f19fe
https://ziglang.org/download/0.15.1/zig-aarch64-macos-0.15.1.tar.xz c4bd624d901c1268f2deb9d8eb2d86a2f8b97bafa3f118025344242da2c54d7b
https://ziglang.org/download/0.15.1/zig-aarch64-windows-0.15.1.zip 1f1bf16228b0ffcc882b713dc5e11a6db4219cb30997e13c72e8e723c2104ec6
https://ziglang.org/download/0.15.1/zig-x86_64-linux-0.15.1.tar.xz c61c5da6edeea14ca51ecd5e4520c6f4189ef5250383db33d01848293bfafe05
https://ziglang.org/download/0.15.1/zig-x86_64-macos-0.15.1.tar.xz 9919392e0287cccc106dfbcbb46c7c1c3fa05d919567bb58d7eb16bca4116184
https://ziglang.org/download/0.15.1/zig-x86_64-windows-0.15.1.zip 91e69e887ca8c943ce9a515df3af013d95a66a190a3df3f89221277ebad29e34
EOF
)

echo "Downloading Zig ${ZIG_RELEASE}..."

case "$(uname -m)" in
    arm64|aarch64)
        ZIG_ARCH="aarch64"
        ;;
    x86_64|amd64)
        ZIG_ARCH="x86_64"
        ;;
    *)
        echo "Unsupported architecture: $(uname -m)"
        exit 1
        ;;
esac

case "$(uname)" in
    Linux)
        ZIG_OS="linux"
        ZIG_EXT=".tar.xz"
        ;;
    Darwin)
        ZIG_OS="macos"
        ZIG_EXT=".tar.xz"
        ;;
    CYGWIN*|MINGW*|MSYS*)
        ZIG_OS="windows"
        ZIG_EXT=".zip"
        ;;
    *)
        echo "Unsupported OS: $(uname)"
        exit 1
        ;;
esac

ZIG_URL="${ZIG_BASE_URL}/${ZIG_RELEASE}/zig-${ZIG_ARCH}-${ZIG_OS}-${ZIG_RELEASE}${ZIG_EXT}"
ZIG_ARCHIVE=$(basename "$ZIG_URL")
ZIG_DIRECTORY=$(basename "$ZIG_ARCHIVE" "$ZIG_EXT")
ZIG_CHECKSUM_EXPECTED=$(echo "$ZIG_CHECKSUMS" | grep -F "$ZIG_URL" | cut -d ' ' -f 2 || true)

if [ -z "$ZIG_CHECKSUM_EXPECTED" ]; then
    echo "No checksum available for ${ZIG_URL}"
    exit 1
fi

if command -v curl >/dev/null 2>&1; then
    curl --location --silent --show-error --output "$ZIG_ARCHIVE" "$ZIG_URL"
elif command -v wget >/dev/null 2>&1; then
    wget --quiet --output-document="$ZIG_ARCHIVE" "$ZIG_URL"
else
    echo "Need curl or wget to download Zig."
    exit 1
fi

if command -v sha256sum >/dev/null 2>&1; then
    ZIG_CHECKSUM_ACTUAL=$(sha256sum "$ZIG_ARCHIVE" | cut -d ' ' -f 1)
elif command -v shasum >/dev/null 2>&1; then
    ZIG_CHECKSUM_ACTUAL=$(shasum -a 256 "$ZIG_ARCHIVE" | cut -d ' ' -f 1)
else
    echo "Need sha256sum or shasum to verify Zig archive."
    exit 1
fi

if [ "$ZIG_CHECKSUM_EXPECTED" != "$ZIG_CHECKSUM_ACTUAL" ]; then
    echo "Checksum mismatch. Expected ${ZIG_CHECKSUM_EXPECTED}, got ${ZIG_CHECKSUM_ACTUAL}."
    rm -f "$ZIG_ARCHIVE"
    exit 1
fi

echo "Extracting ${ZIG_ARCHIVE}..."
case "$ZIG_EXT" in
    .tar.xz)
        tar -xf "$ZIG_ARCHIVE"
        ;;
    .zip)
        if command -v unzip >/dev/null 2>&1; then
            unzip -q "$ZIG_ARCHIVE"
        else
            echo "Need unzip to extract ${ZIG_ARCHIVE}."
            rm -f "$ZIG_ARCHIVE"
            exit 1
        fi
        ;;
    *)
        echo "Unsupported archive type: ${ZIG_EXT}"
        rm -f "$ZIG_ARCHIVE"
        exit 1
        ;;
esac

rm -f "$ZIG_ARCHIVE"

rm -rf doc lib zig LICENSE README.md
mv "$ZIG_DIRECTORY/LICENSE" .
mv "$ZIG_DIRECTORY/README.md" .
mv "$ZIG_DIRECTORY/doc" .
mv "$ZIG_DIRECTORY/lib" .
mv "$ZIG_DIRECTORY/zig" .

rmdir "$ZIG_DIRECTORY"

echo "Zig ${ZIG_RELEASE} downloaded to $(pwd)/zig"
