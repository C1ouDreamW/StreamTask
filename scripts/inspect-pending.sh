#!/usr/bin/env bash
set -euo pipefail

curl -sS "http://localhost:8091/stream-task/admin/pending"
echo
