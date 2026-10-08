# Developer Notes

The repository-root [README.md](../README.md) is the one-stop runbook for a
beginner test. Start there when bringing up the network, services, lifecycle,
invoke, query, and block dump.

This directory is for implementation notes that do not belong in the main
runbook.

## Topics

```text
architecture.md   execution path, helper semantics, remote endorsement, finality
lifecycle.md      package/install/approve/commit/query behavior
resolver.md       CCAAS and remote orchestrator discovery API
```

## Local Fabric-X Network

The local network uses:

```text
Arma orderer        127.0.0.1:6022
Committer sidecar   127.0.0.1:4001
Query Service       127.0.0.1:7001
```

Main scripts:

```bash
./scripts/build-images.sh
./scripts/generate-artifacts.sh
./scripts/start-network.sh
./scripts/create-namespace.sh
./scripts/reset-ledger.sh
./scripts/stop-network.sh
```

`scripts/create-namespace.sh` uses the generated `bin/fxconfig` tool and writes
namespace setup transaction artifacts under `artifacts/fxconfig-tx/`.

## Compatibility Layer Flow

```text
client proposal
  -> orchestrator
  -> helper execution context
  -> shim adapter
  -> external chaincode service
  -> Fabric-X endorsement material
  -> remote orchestrators when policy requires them
  -> result comparison
  -> merged endorsement
  -> orderer submit
  -> finality notification
```

Fabric-X namespace policy is read from Query Service at invocation time. The
lifecycle definition determines the chaincode name/version/sequence and local
package mapping. Connection information is resolved lazily through static config
or a user-owned resolver service.

## Resolver Boundary

The sample resolver is deliberately outside `compatibility_service/`.

The compatibility layer sends a request describing what it needs:

```json
{
  "operation": "REMOTE_ORCHESTRATOR_resolution",
  "requester_msp_id": "org-1",
  "target_msp_id": "org-0",
  "channel_id": "channelqc4",
  "namespace": "1",
  "name": "sample",
  "version": "1.0",
  "sequence": 1
}
```

The resolver returns ordered candidates:

```json
{
  "found": true,
  "candidates": [
    { "msp_id": "org-0", "instance_id": "org0B", "address": "127.0.0.1:9103", "tls_mode": "mtls" },
    { "msp_id": "org-0", "instance_id": "org0A", "address": "127.0.0.1:9102", "tls_mode": "mtls" }
  ]
}
```

The compatibility layer tries candidates in order. Routing policy, health
checks, locality, and load preference belong in the user-owned resolver.

## Smoke Client

The low-level Fabric-X smoke client bypasses chaincode and submits an
`applicationpb.Tx` directly:

```bash
./scripts/smoke.sh
```

To test an MVCC read/write update after an initial write:

```bash
go run ./cmd/rws-smoke --wait --query --key asset2 --value updated --read-version 0
```

`cmd/block-dump` reads committed blocks through the committer sidecar block
query path on port `4001`.
