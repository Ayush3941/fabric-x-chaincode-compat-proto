# Lifecycle Model

The compatibility lifecycle is modeled after Fabric chaincode lifecycle while
remaining small enough for this prototype.

## Commands

```text
orchestrator lifecycle package
orchestrator lifecycle install
orchestrator lifecycle approveformyorg
orchestrator lifecycle checkcommitreadiness
orchestrator lifecycle commit
orchestrator lifecycle queryinstalled
orchestrator lifecycle querycommitted
```

Admin identity comes from `sampleconfig/admin-org0.yaml` or
`sampleconfig/admin-org1.yaml`. The target orchestrator is selected with
`--host`.

## Package

External chaincode packages use the Fabric CCAAS package shape:

```text
package.tgz
  metadata.json
  code.tar.gz
    connection.json
```

The package command reads a directory containing `connection.json` and
`metadata.json`, updates the label, and writes the package archive.

## Install

Install is org-local. It stores package metadata in the target orchestrator's
in-memory lifecycle store.

```text
org0 orchestrator store:
  package_id=org0_sample_1:<hash>
  address=127.0.0.1:9999

org1 orchestrator store:
  package_id=org1_sample_1:<hash>
  address=127.0.0.1:10000
```

Package IDs are local to each organization. Org0 does not need org1's package
ID to execute its own chaincode service.

## Approve

Approve records an org-local approval for:

```text
name
version
sequence
package_id
init_required
approving_msp
```

The current compatibility identity is `name:version`. `sequence` must be the
next lifecycle sequence for that identity.

## Check Commit Readiness

Readiness asks each participating orchestrator whether its local approval
matches the proposed definition.

```text
org-0=true
org-1=true
```

For the prototype, participating orgs come from configured static/dynamic
remote orchestrator resolution. A later production discovery layer can decide
which concrete orchestrator instance should answer for each MSP.

## Commit

Commit verifies readiness and writes a lifecycle marker as a real Fabric-X
transaction in lifecycle namespace `0`.

The shared committed definition stores:

```text
name
version
sequence
init_required
initialized
```

It does not store org-local package IDs or CCAAS addresses. Those remain local
and are resolved by install state or resolver lookup.

## Startup Hydration

On startup, every orchestrator reads lifecycle markers from the Fabric-X ledger
and rebuilds committed definition state locally.

That allows a new orchestrator instance for the same org to start later and
answer `querycommitted` without copying memory from another orchestrator.

## Init Gate

If a definition is committed with `--init-required`, normal invoke/query is
blocked until initialization commits and its initialized lifecycle marker is
confirmed. Initialization requires a committed, init-required definition that
has not already been marked initialized.

The client includes `--is-init` in the signed proposal's
`ChaincodeInput.IsInit` field. Every participating Orchestrator reads that signed
field independently. The legacy `x-compat-is-init` metadata can only assert the
same value; conflicting or invalid metadata is rejected before execution.

The helper carries the flag into the CCaaS shim bridge. Initialization sends
`ChaincodeMessage_INIT` to call Go chaincode's `Init()` method; ordinary requests
send `TRANSACTION` to call `Invoke()`. Query operations reject initialization.
The pinned SDK proposal parser requires at least one argument, including for
initialization; proposals with an empty argument list are not supported.
The original signed proposal, arguments, identity, transient data, and helper
state handling remain available during `Init()` execution. The standard Go
Contract API's `Init()` can delegate to the requested contract function.

After that init transaction commits, the orchestrator writes another lifecycle
marker to mark the definition initialized and propagates the marker to other
organizations. Failed initialization does not mark a definition initialized;
sequential reinitialization is rejected after the marker is applied.

Fabric-X Committer v1.0.4 rejects transaction namespaces with no writes as
`MALFORMED_NO_WRITES`. An `Init()` that returns success without writing state is
therefore submitted but rejected by native validation, and the definition stays
uninitialized. The sample `Init()` initializes a key with the supplied value.

These are sequential lifecycle checks. An atomic ledger guard against concurrent
initialization across Orchestrators is not implemented. Recovery when the
separate initialized-marker transaction times out also remains outstanding.
