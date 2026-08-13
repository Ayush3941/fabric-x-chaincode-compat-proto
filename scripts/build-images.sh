#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/common.sh"

require_bin docker
require_bin go
require_bin git
require_bin make

ORDERER_LOCAL_PATH="${ORDERER_LOCAL_PATH:-${LFX_ROOT}/fabric-x-orderer}"
COMMITTER_LOCAL_PATH="${COMMITTER_LOCAL_PATH:-${LFX_ROOT}/fabric-x-committer}"
FABRIC_X_LOCAL_PATH="${FABRIC_X_LOCAL_PATH:-${LFX_ROOT}/fabric-x}"
BUILD_SRC_DIR="${BUILD_SRC_DIR:-${PROJECT_ROOT}/third_party/.build}"
REFS_CONF="${REFS_CONF:-${LFX_ROOT}/fabric-x/integration/test/refs.conf}"
USE_LOCAL_REPOS="${USE_LOCAL_REPOS:-1}"
FABRIC_X_REF_OVERRIDE="${FABRIC_X_REF:-}"
ORDERER_REF_OVERRIDE="${ORDERER_REF:-}"
COMMITTER_REF_OVERRIDE="${COMMITTER_REF:-}"
FABRIC_X_REPO_OVERRIDE="${FABRIC_X_REPO:-}"
ORDERER_REPO_OVERRIDE="${ORDERER_REPO:-}"
COMMITTER_REPO_OVERRIDE="${COMMITTER_REPO:-}"

mkdir -p "${FABRIC_X_BIN}"
mkdir -p "${PROJECT_ROOT}/.tmp" "${PROJECT_ROOT}/.gocache"

export TMPDIR="${TMPDIR:-${PROJECT_ROOT}/.tmp}"
export GOCACHE="${GOCACHE:-${PROJECT_ROOT}/.gocache}"
DOCKER_BUILD_FLAGS="${DOCKER_BUILD_FLAGS:---no-cache}"
COMMITTER_DOCKER_BUILD_FLAGS="${COMMITTER_DOCKER_BUILD_FLAGS:---no-cache}"
COMMITTER_BUILD_ARCH="${COMMITTER_BUILD_ARCH:-$(go env GOARCH)}"

if [ "${USE_LOCAL_REPOS}" != "1" ] && [ -f "${REFS_CONF}" ]; then
  # shellcheck source=refs.conf
  source "${REFS_CONF}"
fi

FABRIC_X_REF="${FABRIC_X_REF_OVERRIDE:-${FABRIC_X_REF:-v1.0.1}}"
ORDERER_REF="${ORDERER_REF_OVERRIDE:-${ORDERER_REF:-v1.0.3}}"
COMMITTER_REF="${COMMITTER_REF_OVERRIDE:-${COMMITTER_REF:-v1.0.4}}"
FABRIC_X_REPO="${FABRIC_X_REPO_OVERRIDE:-${FABRIC_X_REPO:-https://github.com/hyperledger/fabric-x.git}}"
ORDERER_REPO="${ORDERER_REPO_OVERRIDE:-${ORDERER_REPO:-https://github.com/hyperledger/fabric-x-orderer.git}}"
COMMITTER_REPO="${COMMITTER_REPO_OVERRIDE:-${COMMITTER_REPO:-https://github.com/hyperledger/fabric-x-committer.git}}"

checkout_source() {
  local repo="$1" ref="$2" dest="$3" local_path="$4" name="$5"

  rm -rf "${dest}"
  if [ -n "${local_path}" ] && [ "${USE_LOCAL_REPOS}" = "1" ]; then
    echo "Copying ${name} from local checkout ${local_path}"
    mkdir -p "${dest}"
    (cd "${local_path}" && tar \
      --exclude ./.build \
      --exclude ./bin \
      --exclude ./release \
      --exclude ./tmp \
      --exclude ./.codex \
      --exclude ./integration/test/.build \
      -cf - .) | (cd "${dest}" && tar -xf -)
    return
  fi

  echo "Cloning ${name} ${ref} into ${dest}"
  git clone --depth 1 --branch "${ref}" "${repo}" "${dest}" 2>/dev/null || {
    git clone "${repo}" "${dest}"
    git -C "${dest}" checkout "${ref}"
  }
}

mkdir -p "${BUILD_SRC_DIR}"

PINNED_FABRIC_X="${BUILD_SRC_DIR}/fabric-x"
PINNED_ORDERER="${BUILD_SRC_DIR}/fabric-x-orderer"
PINNED_COMMITTER="${BUILD_SRC_DIR}/fabric-x-committer"

checkout_source "${FABRIC_X_REPO}" "${FABRIC_X_REF}" "${PINNED_FABRIC_X}" "${FABRIC_X_LOCAL_PATH}" "fabric-x"
checkout_source "${ORDERER_REPO}" "${ORDERER_REF}" "${PINNED_ORDERER}" "${ORDERER_LOCAL_PATH}" "fabric-x-orderer"
checkout_source "${COMMITTER_REPO}" "${COMMITTER_REF}" "${PINNED_COMMITTER}" "${COMMITTER_LOCAL_PATH}" "fabric-x-committer"

ORDERER_COMPAT_PATCH="${PROJECT_ROOT}/patches/fabric-x-orderer-use-fabricx-block-data-hash.patch"
ORDERER_HASH_FILE="${PINNED_ORDERER}/vendor/github.com/hyperledger/fabric-x-common/protoutil/blockutils.go"
ORDERER_CONSENSUS_BUILDER="${PINNED_ORDERER}/node/consensus/consensus_builder.go"
if grep -q 'bytes.Join(b.Data, nil)' "${ORDERER_HASH_FILE}" &&
  grep -q '"github.com/hyperledger/fabric/protoutil"' "${ORDERER_CONSENSUS_BUILDER}"; then
  echo "Applying Project orderer compatibility patch"
  git -C "${PINNED_ORDERER}" apply --ignore-space-change "${ORDERER_COMPAT_PATCH}"
else
  echo "Skipping Project orderer compatibility patch; source already matches the updated Fabric-X block/signature format"
fi

echo "Building Fabric-X tools into ${FABRIC_X_BIN}"
rm -rf "${FABRIC_X_BIN}"
mkdir -p "${FABRIC_X_BIN}"
for tool in cryptogen configtxgen fxconfig; do
  make -C "${PINNED_FABRIC_X}" BUILD_DIR="${FABRIC_X_BIN}" "${tool}"
done

echo "Building Arma all-in-one orderer image: ${ORDERER_IMAGE}"
docker build \
  ${DOCKER_BUILD_FLAGS} \
  -t "${ORDERER_IMAGE}" \
  -f "${PINNED_ORDERER}/node/examples/all-in-one/Dockerfile" \
  "${PINNED_ORDERER}"

echo "Building committer test node image: ${COMMITTER_IMAGE}"
make -C "${PINNED_COMMITTER}" clean
make -C "${PINNED_COMMITTER}" \
  BUILD_ARCH="${COMMITTER_BUILD_ARCH}" \
  docker_build_flags="${COMMITTER_DOCKER_BUILD_FLAGS}" \
  build-image-test-node
docker tag docker.io/hyperledger/committer-test-node "${COMMITTER_IMAGE}"

echo "Images ready:"
echo "  ORDERER_IMAGE=${ORDERER_IMAGE}"
echo "  COMMITTER_IMAGE=${COMMITTER_IMAGE}"
echo "Tools ready under ${FABRIC_X_BIN}"
