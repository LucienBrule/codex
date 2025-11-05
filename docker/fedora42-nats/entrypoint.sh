#!/usr/bin/env bash
set -euo pipefail

JETSTREAM_DIR="${NATS_JETSTREAM_DIR:-/jetstream}"
NATS_BIN="/usr/local/bin/nats-server"

mkdir -p "${JETSTREAM_DIR}"
chown -R codex:codex "${JETSTREAM_DIR}"
chmod 0770 "${JETSTREAM_DIR}"

cmd=()
user_args=()
default_mode=0

if [[ $# -eq 0 ]]; then
  default_mode=1
elif [[ "$1" == "nats-server" ]]; then
  shift
  if [[ $# -gt 0 ]]; then
    case "$1" in
      --help|-h|help|--version|-V|version)
        cmd=("${NATS_BIN}" "$@")
        ;;
      *)
        default_mode=1
        user_args=("$@")
        ;;
    esac
  else
    default_mode=1
  fi
else
  cmd=("$@")
fi

if [[ ${default_mode} -eq 1 ]]; then
  cmd=("${NATS_BIN}" "-js" "-sd" "${JETSTREAM_DIR}")

  if [[ -n "${NATS_CONFIG:-}" ]]; then
    cmd+=("-c" "${NATS_CONFIG}")
  fi
  if [[ -n "${NATS_SERVER_NAME:-}" ]]; then
    cmd+=("--server_name=${NATS_SERVER_NAME}")
  fi
  if [[ -n "${NATS_CLUSTER_ADVERTISE:-}" ]]; then
    cmd+=("--cluster_advertise=${NATS_CLUSTER_ADVERTISE}")
  fi
  if [[ -n "${NATS_AUTH_TOKEN:-}" ]]; then
    cmd+=("--auth" "${NATS_AUTH_TOKEN}")
  fi
  if [[ -n "${NATS_USER:-}" ]]; then
    cmd+=("--user" "${NATS_USER}")
  fi
  if [[ -n "${NATS_PASSWORD:-}" ]]; then
    cmd+=("--pass" "${NATS_PASSWORD}")
  fi
  if [[ -n "${NATS_HTTP_USER:-}" ]]; then
    cmd+=("--http_user" "${NATS_HTTP_USER}")
  fi
  if [[ -n "${NATS_HTTP_PASSWORD:-}" ]]; then
    cmd+=("--http_pass" "${NATS_HTTP_PASSWORD}")
  fi
  if [[ -n "${NATS_PORT:-}" ]]; then
    cmd+=("--port" "${NATS_PORT}")
  fi
  if [[ -n "${NATS_CLUSTER_PORT:-}" ]]; then
    cmd+=("--cluster" "0.0.0.0:${NATS_CLUSTER_PORT}")
  fi
  if [[ -n "${NATS_HTTP_PORT:-}" ]]; then
    cmd+=("--http_port" "${NATS_HTTP_PORT}")
  fi
  if [[ -n "${NATS_EXTRA_ARGS:-}" ]]; then
    # shellcheck disable=SC2206
    read -r -a extra_args <<< "${NATS_EXTRA_ARGS}"
    cmd+=("${extra_args[@]}")
  fi
  if [[ ${#user_args[@]} -gt 0 ]]; then
    cmd+=("${user_args[@]}")
  fi
fi

export HOME="/home/codex"
export USER="codex"

if command -v setpriv >/dev/null 2>&1; then
  exec /usr/bin/tini -g -- setpriv --reuid=1000 --regid=1000 --init-groups -- "${cmd[@]}"
fi

printf -v joined ' %q' "${cmd[@]}"
exec /usr/bin/tini -g -- su -s /bin/bash codex -c "exec${joined}"
