# Compatibility Service

This module contains the client CLI, orchestrator server, embedded helper path,
shim bridge, lifecycle store, and resolver clients used by the repository-root
demo.

Use the root [README.md](../README.md) for the full end-to-end runbook.

## Binaries

```text
cmd/client         custom invoke/query and Gateway SDK evaluate
cmd/orchestrator   serves ProcessProposal, Gateway transaction/event APIs, and lifecycle APIs
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
pkg/lifecycle      package construction, install/approve/commit/query state and RPC service
pkg/ccresolver     JSON-over-gRPC resolver API shared with sample resolvers
pkg/config         YAML config structs and TLS/client helpers
```

## Execution Path

```text
client CLI
  -> orchestrator ProcessProposal
  -> verify original client proposal and authorize chaincode/namespace
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

`client evaluate` reuses the existing client configuration and transaction JSON
while calling the official Go Gateway SDK. It constructs contract name
`<name>:<namespace>` from the usual flags, prints the raw chaincode response,
and accepts `--target-orgs` for explicit organization selection. `Evaluate` and
custom Query share execution logic in the existing Orchestrator files.
Gateway `Endorse` and custom Invoke also share their execution and endorsement
stage. Endorse returns an unsigned prepared envelope understood by the SDK;
Invoke continues into submission and finality. Gateway `Submit` verifies the
client-signed prepared envelope and native namespace policy, then uses the same
ordering stage as Invoke. It returns an ordering acknowledgment without waiting
for finality. `CommitStatus` verifies the signed status request and returns a
recorded outcome or waits for finality within that request. Notification recovery
queries stop when the status request ends. Gateway `ChaincodeEvents` reuses
event decoding with custom Invoke and reads committed blocks for live delivery,
replay, and checkpoints. It authenticates subscribers independently of execution
and stops its backend cursor when a subscriber disconnects. See
[Gateway evaluation](../docs/architecture.md#gateway-evaluate) and
[Gateway endorsement](../docs/architecture.md#gateway-endorse), followed by
[Gateway submission](../docs/architecture.md#gateway-submit) and
[Gateway commit status](../docs/architecture.md#gateway-commitstatus), and
[Gateway events](../docs/architecture.md#gateway-chaincodeevents) for the protocol,
and the root runbook for runnable commands.

The transaction store retains pending requests only. Confirmed final outcomes
and definite failures are removed; historical Invoke/Submit responses are not
cached. Clients can read old outcomes through `CommitStatus`. Later submissions
can trigger one bounded asynchronous maintenance pass without waiting for it.
Unfinished lifecycle markers or mirror updates use separate small pending tasks.
See [Finality](../docs/architecture.md#finality) and
[Lifecycle finality recovery](../docs/lifecycle.md#finality-recovery).

## Lifecycle Model

The lifecycle CLI is intentionally close to Fabric lifecycle commands:

```text
orchestrator lifecycle install --path DIRECTORY
orchestrator lifecycle approveformyorg
orchestrator lifecycle checkcommitreadiness
orchestrator lifecycle commit
orchestrator lifecycle queryinstalled
orchestrator lifecycle querycommitted
```

Install reads a local directory containing `connection.json` and `metadata.json`
and builds the signed install request in memory. `--label` optionally overrides
the metadata label without changing the source files. No archive file or separate
package command is needed. Rebuild the Orchestrator binary before using this CLI.
See [folder installation](../docs/lifecycle.md#install) for details.

Installed packages are org-local and in memory. Committed definitions are
written as Fabric-X transactions in lifecycle namespace `0`, then reloaded by
new orchestrators during startup.

The current chaincode identity is:

```text
channel + chaincode name
```

`sequence` advances across versions for that identity. Version is a descriptive
definition property; invocations select the active definition by name. Approving
the next sequence preserves the current sequence's org-local package binding.
See [lifecycle revisions and metadata](../docs/lifecycle.md#commit).

## Resolver Model

CCAAS and remote orchestrator endpoints can be configured statically or resolved
lazily through the resolver API.

Remote orchestrator resolution is MSP-based:

```text
namespace policy says: need org-0
resolver returns: ordered org-0 orchestrator candidates
orchestrator verifies: distinct signer identities until the policy is satisfied
```

Returned certificates are validated against `proposal-auth.msps`; namespace
policies determine which roles and identities must endorse. Multiple signers
from the same MSP can be collected when needed, and the final signature set is
verified locally before submission. Each instance uses its configured signing
identity, so multiple endpoints sharing a certificate count as one signer.

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

Orchestrator configs require `proposal-auth.msps` and `proposal-auth.rules`.
These configure enrollment MSP trust and invocation permissions, independently
of connection TLS. See [proposal authentication](../docs/architecture.md#proposal-authentication)
for the configuration and rejection behavior.

Lifecycle RPCs also require `lifecycle-auth.policies` and
`lifecycle-auth.orchestrators`, using the same trusted MSPs. The admin client
configurations include `channel-id` for operation-bound request signatures.
Rebuild both client and server when upgrading from payload-only lifecycle
signatures. See [lifecycle permissions](../docs/lifecycle.md#authentication-and-permissions).

## Tests

```bash
go test ./pkg/lifecycle ./pkg/orchestrator ./pkg/shim
go test -tags=e2e ./integration/e2e -count=1 -v
```

Run the e2e tests after the Fabric-X network is up. The test harness starts its
own orchestrators and CCAAS processes. `TestClientProposalAuthentication` checks
both admission paths over mTLS and writes its committed transaction result to
`../runtime/compatibility_service/e2e/proposal-auth-result.json` for block inspection.
