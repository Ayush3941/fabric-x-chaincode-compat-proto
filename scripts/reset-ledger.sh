#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/common.sh"

require_bin docker

remove_with_container_fallback() {
  local path="$1"
  local parent image target

  if [ ! -e "${path}" ]; then
    return
  fi

  rm -rf "${path}" 2>/dev/null && return

  parent="$(dirname "${path}")"
  target="$(basename "${path}")"
  image="$2"

  docker run --rm \
    -v "${parent}:/reset-root" \
    --entrypoint sh \
    "${image}" \
    -c "rm -rf /reset-root/${target}"
}

echo "Stopping Fabric-X containers before resetting ledger state"
"${PROJECT_ROOT}/scripts/stop-network.sh" --docker-only

echo "Removing Arma orderer storage: ${STORAGE_DIR}"
remove_with_container_fallback "${STORAGE_DIR}" "${ORDERER_IMAGE}"

echo "Removing committer ledger/state: ${PROJECT_ROOT}/runtime/committer"
remove_with_container_fallback "${PROJECT_ROOT}/runtime/committer" "${COMMITTER_IMAGE}"

mkdir -p "${STORAGE_DIR}" "${PROJECT_ROOT}/runtime/logs"
for i in 1 2 3 4; do
  for role in router assembler batcher consenter; do
    mkdir -p "${STORAGE_DIR}/party${i}/${role}"
  done
done

echo "Ledger reset complete. Artifacts were preserved."
echo "Next:"
echo "  ./scripts/start-network.sh"
echo "  ./scripts/create-namespace.sh"
