#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/common.sh"

[ -x "${FABRIC_X_BIN}/fxconfig" ] || {
  echo "ERROR: ${FABRIC_X_BIN}/fxconfig is missing. Run scripts/build-images.sh first." >&2
  exit 1
}

FXCONFIG_ORG0="${ARTIFACTS_DIR}/fxconfig-peer-org-0.yaml"
FXCONFIG_ORG1="${ARTIFACTS_DIR}/fxconfig-peer-org-1.yaml"
THRESHOLD_POLICY_CERT="${ARTIFACTS_DIR}/peerOrganizations/peer-org-0/users/client@peer-org-0/msp/signcerts/client@peer-org-0-cert.pem"
mkdir -p "${ARTIFACTS_DIR}/fxconfig-tx"

sed "s|ARTIFACTS_DIR|${ARTIFACTS_DIR}|g" \
  "${PROJECT_ROOT}/fxconfig/peer-org-0.yaml" >"${FXCONFIG_ORG0}"
sed "s|ARTIFACTS_DIR|${ARTIFACTS_DIR}|g" \
  "${PROJECT_ROOT}/fxconfig/peer-org-1.yaml" >"${FXCONFIG_ORG1}"

run_fxconfig() {
  FABRIC_LOGGING_SPEC="${FABRIC_LOGGING_SPEC:-error}" "${FABRIC_X_BIN}/fxconfig" "$@"
}

namespace_exists() {
  local namespace="$1"
  local output status

  set +e
  output="$(run_fxconfig namespace list --config="${FXCONFIG_ORG0}" 2>&1)"
  status=$?
  set -e

  if [ "${status}" -ne 0 ]; then
    return 1
  fi

  grep -Eq "^[[:space:]]*[0-9]+\\) ${namespace}: " <<<"${output}"
}

create_namespace() {
  local namespace="$1"
  local policy="$2"
  local tx_dir="${ARTIFACTS_DIR}/fxconfig-tx/namespace-${namespace}"

  if namespace_exists "${namespace}"; then
    echo "Namespace ${namespace} already exists; skipping"
    return
  fi

  rm -rf "${tx_dir}"
  mkdir -p "${tx_dir}"

  echo "Creating namespace ${namespace} with policy ${policy}"
  run_fxconfig namespace create "${namespace}" \
    --config="${FXCONFIG_ORG0}" \
    --policy="${policy}" \
    --output="${tx_dir}/tx.json"

  run_fxconfig tx endorse "${tx_dir}/tx.json" \
    --config="${FXCONFIG_ORG0}" \
    --output="${tx_dir}/tx_org0.json" </dev/null

  run_fxconfig tx endorse "${tx_dir}/tx.json" \
    --config="${FXCONFIG_ORG1}" \
    --output="${tx_dir}/tx_org1.json" </dev/null

  run_fxconfig tx merge \
    "${tx_dir}/tx_org0.json" \
    "${tx_dir}/tx_org1.json" \
    --output="${tx_dir}/tx_merged.json" </dev/null

  local submit_tx="${tx_dir}/tx_merged.json"

  set +e
  SUBMIT_OUTPUT="$(run_fxconfig tx submit --wait \
    "${submit_tx}" \
    --config="${FXCONFIG_ORG0}" </dev/null 2>&1)"
  SUBMIT_STATUS=$?
  set -e
  echo "${SUBMIT_OUTPUT}"
  if [ "${SUBMIT_STATUS}" -ne 0 ]; then
    echo "ERROR: namespace ${namespace} transaction submission failed" >&2
    exit "${SUBMIT_STATUS}"
  fi
  if ! grep -q "Transaction status: COMMITTED" <<<"${SUBMIT_OUTPUT}"; then
    echo "ERROR: namespace ${namespace} transaction did not commit" >&2
    exit 1
  fi

  echo "Namespace ${namespace} is ready"
}

if [ -n "${NAMESPACE:-}" ] || [ -n "${POLICY:-}" ]; then
  namespace="${NAMESPACE:-0}"
  policy="${POLICY:-OR('org-0.member')}"
  create_namespace "${namespace}" "${policy}"
else
  create_namespace "0" "OR('org-0.member')"
  create_namespace "1" "AND('org-0.member','org-1.member')"
  create_namespace "2" "threshold:${THRESHOLD_POLICY_CERT}"
fi
