#!/usr/bin/env bash
set -euo pipefail

# Provide a predictable default command that drops into a shell when no args are supplied.
if [[ $# -eq 0 ]]; then
  set -- /bin/bash
fi

exec /usr/bin/tini -g -- "$@"
