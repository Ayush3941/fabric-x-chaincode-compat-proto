#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/common.sh"

DOCKER_ONLY="false"
if [ "${1:-}" = "--docker-only" ]; then
  DOCKER_ONLY="true"
fi

docker rm -f project-arma project-committer project-prometheus project-grafana 2>/dev/null || true
docker network rm project_project-fx 2>/dev/null || true

if [ "${DOCKER_ONLY}" != "true" ]; then
  echo "Stopped network. Artifacts and storage were preserved:"
  echo "  ${ARTIFACTS_DIR}"
  echo "  ${STORAGE_DIR}"
fi
