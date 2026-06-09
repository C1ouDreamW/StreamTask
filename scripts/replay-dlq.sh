#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <dlq-message-id>" >&2
  exit 1
fi

curl -sS -X POST "http://localhost:8091/stream-task/admin/dlq/$1/replay"
echo
