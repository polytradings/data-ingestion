#!/usr/bin/env sh
set -eu

. "$(dirname "$0")/compose-env.sh"
docker compose build "$@"
