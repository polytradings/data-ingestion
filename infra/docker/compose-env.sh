#!/usr/bin/env sh
set -eu

ARCH_RAW="$(uname -m)"

case "$ARCH_RAW" in
  x86_64)
    TARGETARCH="amd64"
    ;;
  aarch64|arm64)
    TARGETARCH="arm64"
    ;;
  *)
    TARGETARCH="$ARCH_RAW"
    ;;
esac

export TARGETOS="${TARGETOS:-linux}"
export TARGETARCH

echo "Detected host arch: $ARCH_RAW -> TARGETARCH=$TARGETARCH"
