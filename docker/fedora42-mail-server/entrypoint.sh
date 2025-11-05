#!/usr/bin/env bash
set -euo pipefail

MAIL_SERVER_BIN="${CODEX_MAIL_SERVER_BIN:-/usr/local/bin/codex-mail-server}"
RUNTIME_DIR="${CODEX_MAIL_SERVER_RUNTIME_DIR:-/var/run/codex-mailbox}"
LOG_DIR="${CODEX_MAIL_SERVER_LOG_DIR:-/var/log/codex-mailbox}"
MAIL_SERVER_UID="${CODEX_MAIL_SERVER_UID:-1000}"
MAIL_SERVER_GID="${CODEX_MAIL_SERVER_GID:-1000}"
ENV_FILE="${CODEX_MAIL_SERVER_ENV_FILE:-}"

if [[ ! -x "${MAIL_SERVER_BIN}" ]]; then
  echo "codex-mail-server binary not found at ${MAIL_SERVER_BIN}" >&2
  exit 126
fi

# Handle simple flag requests that should exit immediately.
if [[ $# -gt 0 ]]; then
  case "$1" in
    --version|-V|version)
      printf 'codex-mail-server %s\n' "${CODEX_MAIL_SERVER_VERSION:-dev}"
      exit 0
      ;;
    --help|-h|help)
      printf '%s\n' "codex-mail-server help: see docs/codex/mailbox.md for usage."
      exit 0
      ;;
  esac
fi

# Load optional environment overrides from a mounted file.
if [[ -n "${ENV_FILE}" ]]; then
  if [[ -f "${ENV_FILE}" ]]; then
    # shellcheck disable=SC1090
    set -a && source "${ENV_FILE}" && set +a
  else
    echo "warning: CODEX_MAIL_SERVER_ENV_FILE=${ENV_FILE} does not exist" >&2
  fi
fi

umask 007

install -d -m 0770 "${RUNTIME_DIR}"
install -d -m 0770 "${LOG_DIR}"
chown -R "${MAIL_SERVER_UID}:${MAIL_SERVER_GID}" "${RUNTIME_DIR}" "${LOG_DIR}"

if [[ -z "${CODEX_HOME:-}" ]]; then
  export CODEX_HOME="${RUNTIME_DIR}"
fi

NAMESPACE="${CODEX_NAMESPACE:-codex}"
SOCKET_PATH="${CODEX_MAIL_SERVER_SOCKET:-${CODEX_HOME}/${NAMESPACE}/mailbox/dispatcher.sock}"
REGISTRY_PATH="${CODEX_MAILBOX_REGISTRY_PATH:-${CODEX_HOME}/${NAMESPACE}/mailbox/registry.json}"

install -d -m 0770 "$(dirname "${SOCKET_PATH}")"
install -d -m 0770 "$(dirname "${REGISTRY_PATH}")"
chown -R "${MAIL_SERVER_UID}:${MAIL_SERVER_GID}" \
  "$(dirname "${SOCKET_PATH}")" \
  "$(dirname "${REGISTRY_PATH}")"

if [[ -n "${CODEX_MAIL_SERVER_LOG_FILE:-}" ]]; then
  install -m 0660 /dev/null "${CODEX_MAIL_SERVER_LOG_FILE}"
  chown "${MAIL_SERVER_UID}:${MAIL_SERVER_GID}" "${CODEX_MAIL_SERVER_LOG_FILE}"
fi

cmd=()

if [[ $# -eq 0 ]]; then
  cmd=("${MAIL_SERVER_BIN}")
elif [[ "$1" == "codex-mail-server" ]]; then
  shift
  cmd=("${MAIL_SERVER_BIN}" "$@")
elif [[ "$1" == -* ]]; then
  cmd=("${MAIL_SERVER_BIN}" "$@")
else
  cmd=("$@")
fi

export HOME="/home/codex"
export USER="codex"

if command -v setpriv >/dev/null 2>&1; then
  exec /usr/bin/tini -g -- setpriv --reuid="${MAIL_SERVER_UID}" --regid="${MAIL_SERVER_GID}" --init-groups -- "${cmd[@]}"
fi

printf -v joined ' %q' "${cmd[@]}"
exec /usr/bin/tini -g -- su -s /bin/bash codex -c "exec${joined}"
