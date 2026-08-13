#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/common.sh"

"${PROJECT_ROOT}/scripts/stop-network.sh" --docker-only

rm -rf "${ARTIFACTS_DIR}"
if [ -d "${STORAGE_DIR}" ]; then
  rm -rf "${STORAGE_DIR}" 2>/dev/null || {
    docker run --rm \
      -v "$(dirname "${STORAGE_DIR}"):/storage-root" \
      --entrypoint sh \
      "${ORDERER_IMAGE}" \
      -c "rm -rf /storage-root/$(basename "${STORAGE_DIR}")"
  }
fi
if [ -d "${PROJECT_ROOT}/runtime/committer" ]; then
  rm -rf "${PROJECT_ROOT}/runtime/committer" 2>/dev/null || {
    docker run --rm \
      -v "${PROJECT_ROOT}/runtime:/runtime" \
      --entrypoint sh \
      "${COMMITTER_IMAGE}" \
      -c 'rm -rf /runtime/committer'
  }
fi
mkdir -p "${PROJECT_ROOT}/artifacts" "${PROJECT_ROOT}/storage" "${PROJECT_ROOT}/runtime/logs"

echo "Cleaned Project runtime artifacts"
