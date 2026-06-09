#!/usr/bin/env bash
set -euo pipefail

TYPE="${1:-success}"
curl -sS -X POST "http://localhost:8091/demo/tasks/${TYPE}"
echo
