# Lifecycle Model

The compatibility lifecycle is modeled after Fabric chaincode lifecycle while
remaining small enough for this prototype.

## Commands

```text
orchestrator lifecycle install --path DIRECTORY
orchestrator lifecycle approveformyorg
orchestrator lifecycle checkcommitreadiness
orchestrator lifecycle commit
orchestrator lifecycle queryinstalled
orchestrator lifecycle querycommitted
```

Admin identity comes from `sampleconfig/admin-org0.yaml` or
`sampleconfig/admin-org1.yaml`. The target orchestrator is selected with
`--host`.

## Authentication and Permissions

Lifecycle RPCs authenticate enrollment identities using the same public MSPs in
`proposal-auth.msps` as transaction proposals. Certificate trust, configured
revocation data, certificate validity, and request signatures are checked before
payload processing or state access. TLS connection authentication is separate.

`lifecycle-auth.policies` configures the caller permission for each operation.
The sample configuration follows Fabric's default caller-policy split:

| Operation | Sample caller permission |
|---|---|
| `install`, `queryinstalled`, `approveformyorg` | Administrator of the target Orchestrator's organization |
| `checkcommitreadiness`, `commit`, `querycommitted` | Member of either configured demo organization, representing the writer policy |
| Internal readiness, definition propagation, `markinitialized` | Explicitly configured Orchestrator enrollment identities |

A writer's permission to request commitment does not replace administrator
approvals or the Fabric-X lifecycle namespace's endorsement policy. Installation,
installed-package queries, and organizational approval also require the caller's
verified MSP to match the target organization.

For example, org0 uses these policies:

```yaml
lifecycle-auth:
  policies:
    install: "OR('org-0.admin')"
    queryinstalled: "OR('org-0.admin')"
    approveformyorg: "OR('org-0.admin')"
    checkcommitreadiness: "OR('org-0.member','org-1.member')"
    commit: "OR('org-0.member','org-1.member')"
    querycommitted: "OR('org-0.member','org-1.member')"
```

The same section also requires `orchestrators`, a list of `msp-id` and
`cert-path` entries pointing to permitted public enrollment certificates. See
[the complete org0 configuration](../compatibility_service/sampleconfig/orchestrator.yaml).
These identities must validate under the shared MSP trust configuration. The
local signing identity must be listed too. Sharing an MSP does not automatically
grant internal coordination permission. Trust, policies, and permitted
identities are loaded at startup; restart after changing them.

Lifecycle client configurations include `channel-id`, as shown in the existing
admin configurations. The signed request binds a protocol version, caller
identity, channel, operation, internal-processing flag, timestamp, nonce, and
payload. Requests must be within a five-minute clock window. This bounds request
age; it is not a durable replay-detection store. Payload-only legacy signatures
are rejected. Rebuild the client and server together; CLI command syntax stays
the same.

For coordination, the initiating Orchestrator signs a new internal request for
the specific remote operation after checking the public caller's permissions.
Internal definition and initialization updates read committed lifecycle state
before updating the local mirror. An internal caller cannot activate a
nonexistent definition or an unconfirmed initialized marker. The legacy
`x-compat-lifecycle-local-only` metadata can only match the signed internal flag;
it cannot grant internal permissions or bypass ledger submission.

Missing permission rules, unknown operations, invalid policies, untrusted
Orchestrator certificates, and an unlisted local signer prevent startup.

## Install

Install accepts a local CCAAS metadata directory:

```text
cc_go/org0_sample/
  connection.json
  metadata.json
```

From `compatibility_service`, run:

```bash
./bin/orchestrator lifecycle install -c sampleconfig/admin-org0.yaml --host 127.0.0.1:9102 --path ../sample_external_chaincode/cc_go/org0_sample
```

The CLI reads the directory locally, creates the package in memory, and signs and
sends its contents to the selected Orchestrator. It uses the label in
`metadata.json`; `--label` overrides that label for the request without changing
the source file. When `--label` is supplied, missing metadata can use the default
external-chaincode metadata. Optional `META-INF` contents are included as before.

The binary has no separate `package` command, and `install` no longer accepts an
archive filename. Installation creates no `.tgz` file and does not modify the
source directory. The chaincode executable is still built and started separately.

The signed RPC continues to carry the Fabric CCAAS package shape in memory:

```text
metadata.json
code.tar.gz
  connection.json
```

The Orchestrator independently validates these bytes and derives the package ID
as `label:SHA256(package bytes)`. Reinstalling unchanged metadata with the same
label produces the same package ID. The archive representation and signed install
RPC remain unchanged.

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

The logical identity is the configured channel plus chaincode name. Version is
a descriptive definition property that must match across approvals. Sequence
starts at `1` and advances by exactly one for every committed definition update,
including version changes:

| Name | Version | Sequence |
|---|---|---:|
| sample | 1.0 | 1 |
| sample | 2.0 | 2 |
| sample | 2.1 | 3 |

Approvals are kept separately for each name, sequence, and organization. Preparing
the next revision does not remove the active revision's package binding. An
organization can correct its pending approval or bind a different installed
package to an identical committed definition. It cannot change the properties of
an already committed sequence.

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
channel_id
name
version
sequence
init_required
initialized
```

It does not store org-local package IDs or CCAAS addresses. Those remain local
and are resolved by install state or resolver lookup.

There is one active definition per channel and name. Commit replaces that active
definition at `_lifecycle/channels/<channel>/chaincodes/<name>`; key components
are escaped. A channel-scoped append-only index records lifecycle changes. The
backend checks the next sequence against committed ledger state and records the
current key version for native MVCC validation. A stale local mirror cannot
overwrite a newer definition by reusing an earlier sequence.

Invocation and query select the active definition by name. The client `-v` flag
is optional; it does not select a runtime or restore an old revision. The original
signed proposal is retained, while helper decorations and resolver requests use
the committed version and sequence. Local approved package bindings take
precedence over configured discovery. Resolver caches include the sequence, so
an upgrade resolves its own endpoint. Version changes do not create or clear an
application namespace; keep using the same `--namespace` to retain application
state. Namespace creation and endorsement-policy updates remain separate tasks.

The prior prototype's unscoped `name:version` metadata is not automatically merged:
it can contain independent definitions with overlapping sequences. Reinstall and
approve/commit the intended definition at sequence `1` in the new channel-scoped
format, or reset the development ledger when intentionally starting over.
Existing application state is retained when the ledger and application namespace
are retained. New-format definitions reload normally after restart.

## Finality Recovery

A timed-out definition commit or initialization-marker submission keeps a small
pending operation containing the definition, submitted transaction ID, and any
organizations still awaiting a mirror update. It retains no signed proposal,
endorsements, or completed application history. Once an application Init has a
confirmed outcome, its transaction record is removed even if its lifecycle task
still needs work.

A subsequent submission can trigger a bounded asynchronous recovery pass; it does
not wait for that pass. Recovery queries unknown transaction IDs before taking
another step. Missing status is not proof of rejection and never causes a resend.
A marker that has not yet been submitted can be prepared after the original Init
commits. An existing committed marker is reused without another ledger write.
Only a known marker rejection permits a fresh marker attempt on a later pass.

After ledger confirmation, the same finalization logic updates the local mirror
and sends signed updates only to organizations still outstanding. It never reruns
Init or applies an old initialization result to a newer definition. Finished or
superseded lifecycle work is removed. Recovery has no periodic timer, does not
return old results to a new client request, and is canceled on service shutdown.

Pending tasks are instance-local and lost on restart. Startup hydration still
recovers definitions and initialized markers already committed to the ledger;
it cannot reconstruct an Init marker that was never written before a crash.

## Startup Hydration

On startup, every orchestrator reads lifecycle markers from the Fabric-X ledger
and rebuilds committed definition state locally.

Index entries point to the stable name key. Reloading earlier index entries
therefore restores the latest committed definition instead of activating old
versions alongside it.

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
