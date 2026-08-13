#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/common.sh"

NAMESPACE="${NAMESPACE:-0}"
POLICY="${POLICY:-OR('org-0.member')}"

[ -x "${FABRIC_X_BIN}/fxconfig" ] || {
  echo "ERROR: ${FABRIC_X_BIN}/fxconfig is missing. Run scripts/build-images.sh first." >&2
  exit 1
}

FXCONFIG_ORG0="${ARTIFACTS_DIR}/fxconfig-peer-org-0.yaml"
FXCONFIG_ORG1="${ARTIFACTS_DIR}/fxconfig-peer-org-1.yaml"
mkdir -p "${ARTIFACTS_DIR}/fxconfig-tx"

sed "s|ARTIFACTS_DIR|${ARTIFACTS_DIR}|g" \
  "${PROJECT_ROOT}/fxconfig/peer-org-0.yaml" >"${FXCONFIG_ORG0}"
sed "s|ARTIFACTS_DIR|${ARTIFACTS_DIR}|g" \
  "${PROJECT_ROOT}/fxconfig/peer-org-1.yaml" >"${FXCONFIG_ORG1}"

echo "Creating namespace ${NAMESPACE} with policy ${POLICY}"
"${FABRIC_X_BIN}/fxconfig" namespace create "${NAMESPACE}" \
  --config="${FXCONFIG_ORG0}" \
  --policy="${POLICY}" \
  --output="${ARTIFACTS_DIR}/fxconfig-tx/tx.json"

"${FABRIC_X_BIN}/fxconfig" tx endorse "${ARTIFACTS_DIR}/fxconfig-tx/tx.json" \
  --config="${FXCONFIG_ORG0}" \
  --output="${ARTIFACTS_DIR}/fxconfig-tx/tx_org0.json" </dev/null

"${FABRIC_X_BIN}/fxconfig" tx endorse "${ARTIFACTS_DIR}/fxconfig-tx/tx.json" \
  --config="${FXCONFIG_ORG1}" \
  --output="${ARTIFACTS_DIR}/fxconfig-tx/tx_org1.json" </dev/null

"${FABRIC_X_BIN}/fxconfig" tx merge \
  "${ARTIFACTS_DIR}/fxconfig-tx/tx_org0.json" \
  "${ARTIFACTS_DIR}/fxconfig-tx/tx_org1.json" \
  --output="${ARTIFACTS_DIR}/fxconfig-tx/tx_merged.json" </dev/null

set +e
SUBMIT_OUTPUT="$("${FABRIC_X_BIN}/fxconfig" tx submit --wait \
  "${ARTIFACTS_DIR}/fxconfig-tx/tx_merged.json" \
  --config="${FXCONFIG_ORG0}" </dev/null 2>&1)"
SUBMIT_STATUS=$?
set -e
echo "${SUBMIT_OUTPUT}"
if [ "${SUBMIT_STATUS}" -ne 0 ]; then
  echo "ERROR: namespace transaction submission failed" >&2
  exit "${SUBMIT_STATUS}"
fi
if ! grep -q "Transaction status: COMMITTED" <<<"${SUBMIT_OUTPUT}"; then
  echo "ERROR: namespace transaction did not commit" >&2
  exit 1
fi

echo "Namespace ${NAMESPACE} is ready"
