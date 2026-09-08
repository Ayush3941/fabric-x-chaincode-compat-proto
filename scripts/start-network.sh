#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/common.sh"

require_bin docker
require_bin nc
require_bin openssl

if [ ! -f "${ARTIFACTS_DIR}/config-block.pb.bin" ]; then
  echo "ERROR: missing ${ARTIFACTS_DIR}/config-block.pb.bin. Run scripts/generate-artifacts.sh first." >&2
  exit 1
fi

echo "Starting real Fabric-X orderer and committer services"
compose up -d arma committer

wait_for_port 6022 "Arma router"
wait_for_port 6024 "Arma batcher"
wait_for_port 4001 "Committer sidecar"
wait_for_port 7001 "Query service"

MTLS_CERT="${ARTIFACTS_DIR}/peerOrganizations/peer-org-0/peers/helper.peer-org-0/tls/server.crt"
MTLS_KEY="${ARTIFACTS_DIR}/peerOrganizations/peer-org-0/peers/helper.peer-org-0/tls/server.key"

wait_for_mtls \
  6022 \
  "Arma router" \
  "${MTLS_CERT}" \
  "${MTLS_KEY}" \
  "${ARTIFACTS_DIR}/ordererOrganizations/orderer-org-1/msp/tlscacerts/tlsca.orderer-org-1-cert.pem"

wait_for_mtls \
  4001 \
  "Committer sidecar" \
  "${MTLS_CERT}" \
  "${MTLS_KEY}" \
  "${ARTIFACTS_DIR}/peerOrganizations/peer-org-0/msp/tlscacerts/tlsca.peer-org-0-cert.pem"

wait_for_container_log \
  "project-committer" \
  "Starting coordinator sender and receiver" \
  "Committer delivery path" \
  180

echo "Network is up"
