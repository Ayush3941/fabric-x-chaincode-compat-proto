#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/common.sh"

NAMESPACE="${NAMESPACE:-0}"
KEY="${KEY:-asset1}"
VALUE="${VALUE:-value-$(date +%s)}"

echo "Submitting smoke write namespace=${NAMESPACE} key=${KEY} value=${VALUE}"
cd "${PROJECT_ROOT}"
go run ./cmd/rws-smoke \
  --artifacts "${ARTIFACTS_DIR}" \
  --namespace "${NAMESPACE}" \
  --key "${KEY}" \
  --value "${VALUE}" \
  --wait \
  --query
