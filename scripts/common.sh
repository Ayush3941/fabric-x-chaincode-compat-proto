#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LFX_ROOT="$(cd "${PROJECT_ROOT}/.." && pwd)"

export PROJECT_ROOT
export LFX_ROOT
export ARTIFACTS_DIR="${ARTIFACTS_DIR:-${PROJECT_ROOT}/artifacts}"
export STORAGE_DIR="${STORAGE_DIR:-${PROJECT_ROOT}/storage/arma}"
export FABRIC_X_BIN="${FABRIC_X_BIN:-${PROJECT_ROOT}/bin}"

export ORDERER_IMAGE="${ORDERER_IMAGE:-docker.io/hyperledger/arma-4p1s:project-local}"
export COMMITTER_IMAGE="${COMMITTER_IMAGE:-docker.io/hyperledger/committer-test-node:project-local}"

COMPOSE_FILE="${PROJECT_ROOT}/docker-compose.yaml"
export COMPOSE_FILE

require_bin() {
  local bin="$1"
  command -v "${bin}" >/dev/null 2>&1 || {
    echo "ERROR: '${bin}' is required but not found in PATH" >&2
    exit 1
  }
}

wait_for_port() {
  local port="$1" name="$2" timeout="${3:-120}"
  echo "Waiting for ${name} on 127.0.0.1:${port}..."
  local i
  for ((i = 1; i <= timeout; i++)); do
    nc -z 127.0.0.1 "${port}" 2>/dev/null && return 0
    sleep 1
  done
  echo "ERROR: timed out waiting for ${name}" >&2
  return 1
}

wait_for_mtls() {
  local port="$1" name="$2" cert="$3" key="$4" ca="$5" timeout="${6:-120}"
  echo "Waiting for ${name} TLS on 127.0.0.1:${port}..."
  local i
  for ((i = 1; i <= timeout; i++)); do
    if echo | openssl s_client \
      -connect "127.0.0.1:${port}" \
      -cert "${cert}" \
      -key "${key}" \
      -CAfile "${ca}" \
      -servername 127.0.0.1 \
      2>/dev/null | grep -q "Verify return code: 0 (ok)"; then
      return 0
    fi
    sleep 1
  done
  echo "ERROR: timed out waiting for ${name} TLS" >&2
  return 1
}

wait_for_container_log() {
  local container="$1" pattern="$2" name="$3" timeout="${4:-120}"
  echo "Waiting for ${name} readiness log..."
  local i
  for ((i = 1; i <= timeout; i++)); do
    if docker logs "${container}" 2>&1 | grep -q "${pattern}"; then
      return 0
    fi
    sleep 1
  done
  echo "ERROR: timed out waiting for ${name} readiness log" >&2
  return 1
}

compose() {
  docker compose -f "${COMPOSE_FILE}" "$@"
}
