#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/common.sh"

require_bin docker
require_bin sed

for bin in cryptogen configtxgen fxconfig; do
  [ -x "${FABRIC_X_BIN}/${bin}" ] || {
    echo "ERROR: ${FABRIC_X_BIN}/${bin} is missing. Run scripts/build-images.sh first." >&2
    exit 1
  }
done

echo "Cleaning old artifacts and storage"
"${PROJECT_ROOT}/scripts/stop-network.sh" --docker-only >/dev/null 2>&1 || true
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
mkdir -p "${ARTIFACTS_DIR}" "${STORAGE_DIR}"

for i in 1 2 3 4; do
  for role in router assembler batcher consenter; do
    mkdir -p "${STORAGE_DIR}/party${i}/${role}"
  done
done

echo "Generating crypto material"
"${FABRIC_X_BIN}/cryptogen" generate \
  --config="${PROJECT_ROOT}/networkconfig/crypto-config.yaml" \
  --output="${ARTIFACTS_DIR}"

echo "Generating Arma shared config proto"
sed "s|ARTIFACTS_DIR|/artifacts|g" \
  "${PROJECT_ROOT}/networkconfig/arma_config.yaml" >"${ARTIFACTS_DIR}/shared_config.yaml"
mkdir -p "${ARTIFACTS_DIR}/bootstrap"

DOCKER_USER_ARGS=()
if [ "$(uname)" = "Linux" ]; then
  DOCKER_USER_ARGS=("--user" "$(id -u):$(id -g)")
fi

docker run --rm --entrypoint armageddon \
  ${DOCKER_USER_ARGS[@]+"${DOCKER_USER_ARGS[@]}"} \
  -v "${ARTIFACTS_DIR}:/artifacts" \
  "${ORDERER_IMAGE}" \
  createSharedConfigProto \
  --sharedConfigYaml="/artifacts/shared_config.yaml" \
  --output="/artifacts/bootstrap"

echo "Generating per-role Arma local configs"
CONTAINER_ARTIFACTS="/tmp/arma-all-in-one"
PEER_CA_EXTRA=$(printf '\\\n      - %s\\\n      - %s' \
  "${CONTAINER_ARTIFACTS}/peerOrganizations/peer-org-0/msp/tlscacerts/tlsca.peer-org-0-cert.pem" \
  "${CONTAINER_ARTIFACTS}/peerOrganizations/peer-org-1/msp/tlscacerts/tlsca.peer-org-1-cert.pem")

for i in 1 2 3 4; do
  PARTY_DIR="${ARTIFACTS_DIR}/config/party${i}"
  mkdir -p "${PARTY_DIR}"
  OFFSET=$(((i - 1) * 100))
  ORG_DOMAIN="orderer-org-${i}"
  PARTY="party${i}"

  for role_tpl in router assembler batcher consenter; do
    case ${role_tpl} in
    router) PORT=$((6022 + OFFSET)) ;;
    assembler) PORT=$((6023 + OFFSET)) ;;
    batcher) PORT=$((6024 + OFFSET)) ;;
    consenter) PORT=$((6025 + OFFSET)) ;;
    esac

    if [ "${role_tpl}" = "batcher" ]; then
      NODE_DIR="batcher1.${ORG_DOMAIN}"
    else
      NODE_DIR="${role_tpl}.${ORG_DOMAIN}"
    fi

    EXTRA_CAS=""
    if [ "${role_tpl}" = "router" ] || [ "${role_tpl}" = "assembler" ]; then
      EXTRA_CAS="${PEER_CA_EXTRA}"
    fi

    cat "${PROJECT_ROOT}/ordererconfig/base.yaml.tpl" \
      "${PROJECT_ROOT}/ordererconfig/role_${role_tpl}.yaml" |
      sed \
        -e "s|ARTIFACTS_DIR|${CONTAINER_ARTIFACTS}|g" \
        -e "s|PORT|${PORT}|g" \
        -e "s|ORG_DOMAIN|${ORG_DOMAIN}|g" \
        -e "s|ORG_MSP_ID|OrdererOrg${i}MSP|g" \
        -e "s|PARTY_ID|${i}|g" \
        -e "s|PARTY|${PARTY}|g" \
        -e "s|NODE_DIR|${NODE_DIR}|g" \
        -e "s|STORAGE_DIR|/storage/party${i}/${role_tpl}|g" \
        -e "s|CLIENT_ROOT_CAS_EXTRA|${EXTRA_CAS}|g" \
        >"${PARTY_DIR}/local_config_${NODE_DIR%%.*}.yaml"

    OPERATIONS_PORT=$((8000 + OFFSET))
    case ${role_tpl} in
    router) OPERATIONS_PORT=$((OPERATIONS_PORT + 22)) ;;
    assembler) OPERATIONS_PORT=$((OPERATIONS_PORT + 23)) ;;
    batcher) OPERATIONS_PORT=$((OPERATIONS_PORT + 24)) ;;
    consenter) OPERATIONS_PORT=$((OPERATIONS_PORT + 25)) ;;
    esac

    printf '\nOperations:\n  ListenAddress: 0.0.0.0\n  ListenPort: %d\n' "${OPERATIONS_PORT}" \
      >>"${PARTY_DIR}/local_config_${NODE_DIR%%.*}.yaml"
  done
done

echo "Generating channel config block"
CONFIGTX_DIR="${ARTIFACTS_DIR}/networkconfig"
mkdir -p "${CONFIGTX_DIR}"
sed "s|ARTIFACTS_DIR|${ARTIFACTS_DIR}|g" \
  "${PROJECT_ROOT}/networkconfig/configtx.yaml" >"${CONFIGTX_DIR}/configtx.yaml"

"${FABRIC_X_BIN}/configtxgen" \
  -profile E2EProfile \
  -channelID channelqc4 \
  -configPath "${CONFIGTX_DIR}" \
  -outputBlock "${ARTIFACTS_DIR}/config-block.pb.bin"

find "${ARTIFACTS_DIR}" -type d -exec chmod a+rx {} +
find "${ARTIFACTS_DIR}" -type f -exec chmod a+r {} +

echo "Artifacts generated under ${ARTIFACTS_DIR}"
