# Compatibility Service

This module contains the client CLI, orchestrator server, embedded helper path,
shim bridge, lifecycle store, and resolver clients used by the repository-root
demo.

Use the root [README.md](../README.md) for the full end-to-end runbook.

## Binaries

```text
cmd/client         sends MSP-signed invoke/query proposals to an orchestrator
cmd/orchestrator   serves ProcessProposal and lifecycle APIs
```

Build:

```bash
go build -o bin/client ./cmd/client
go build -o bin/orchestrator ./cmd/orchestrator
```

## Packages

```text
pkg/orchestrator   coordinates execution, lifecycle, policy, submit, finality
pkg/helper         runs one chaincode execution and builds Fabric-X endorsement material
pkg/shim           speaks Fabric CCAAS shim messages to the external chaincode
pkg/lifecycle      package/install/approve/commit/query state and RPC service
pkg/ccresolver     JSON-over-gRPC resolver API shared with sample resolvers
pkg/config         YAML config structs and TLS/client helpers
```

## Execution Path

```text
client CLI
  -> orchestrator ProcessProposal
  -> local helper
  -> shim adapter
  -> external chaincode service
  -> Fabric-X endorsement material
  -> remote orchestrators when namespace policy requires them
  -> canonical result comparison
  -> endorsement merge
  -> Fabric-X orderer submit
  -> Notification Service finality
```

The helper does not write committed state directly. It reads committed values
through the Fabric-X Query Service and records local read/write effects in the
execution context. Those effects are converted into Fabric-X endorsement
material after chaincode returns.

## Lifecycle Model

The lifecycle CLI is intentionally close to Fabric lifecycle commands:

```text
orchestrator lifecycle package
orchestrator lifecycle install
orchestrator lifecycle approveformyorg
orchestrator lifecycle checkcommitreadiness
orchestrator lifecycle commit
orchestrator lifecycle queryinstalled
orchestrator lifecycle querycommitted
```

Installed packages are org-local and in memory. Committed definitions are
written as Fabric-X transactions in lifecycle namespace `0`, then reloaded by
new orchestrators during startup.

The current chaincode identity is:

```text
name:version
```

`sequence` is used as the lifecycle upgrade counter for that identity.

## Resolver Model

CCAAS and remote orchestrator endpoints can be configured statically or resolved
lazily through the resolver API.

Remote orchestrator resolution is MSP-based:

```text
namespace policy says: need org-0
resolver returns: ordered org-0 orchestrator candidates
orchestrator tries: first candidate, then fallback candidates
```

The resolver request includes `requester_msp_id`, `target_msp_id`, channel,
namespace, chaincode name, version, and sequence. A user-owned resolver can use
those fields for health checks, locality, routing, load preference, or failover.

## Config Files

```text
sampleconfig/client.yaml          org0 client CLI config
sampleconfig/client-org1.yaml     org1 client CLI config
sampleconfig/orchestrator.yaml    org0 orchestrator on 127.0.0.1:9102
sampleconfig/orchestrator0B.yaml  second org0 orchestrator on 127.0.0.1:9103
sampleconfig/orchestrator1.yaml   org1 orchestrator on 127.0.0.1:9202
sampleconfig/admin-org0.yaml      org0 lifecycle admin identity
sampleconfig/admin-org1.yaml      org1 lifecycle admin identity
```

Admin configs intentionally do not pin an orchestrator endpoint. Pass the target
with `--host`:

```bash
./bin/orchestrator lifecycle querycommitted -c sampleconfig/admin-org0.yaml --host 127.0.0.1:9102 -n sample -v 1.0
```

## Tests

```bash
go test ./pkg/lifecycle ./pkg/orchestrator ./pkg/shim
go test -tags=e2e ./integration/e2e -count=1 -v
```

Run the e2e tests only after the Fabric-X network and sample services are up.
