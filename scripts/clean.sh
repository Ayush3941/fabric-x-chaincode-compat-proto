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
rm -rf \
  "${PROJECT_ROOT}/runtime/compatibility_service" \
  "${PROJECT_ROOT}/compatibility_service/runtime" \
  "${PROJECT_ROOT}/sample_external_chaincode/runtime" \
  "${PROJECT_ROOT}/compatibility_service/bin" \
  "${PROJECT_ROOT}/sample_external_chaincode/bin" \
  "${PROJECT_ROOT}/bin/block-dump" \
  "${PROJECT_ROOT}/bin/rws-smoke" \
  "${PROJECT_ROOT}/.tmp" \
  "${PROJECT_ROOT}/.gocache" \
  "${PROJECT_ROOT}/third_party/.build"
mkdir -p "${PROJECT_ROOT}/artifacts" "${PROJECT_ROOT}/storage" "${PROJECT_ROOT}/runtime/logs"

echo "Cleaned Project generated artifacts, runtime state, and build caches"
