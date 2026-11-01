# Compatibility Architecture

This note describes the compatibility execution path inside one invocation.
Use the repository-root README for runnable commands.

## Services

```text
client CLI
orchestrator
embedded helper
shim adapter
external chaincode service
Fabric-X Query Service
Fabric-X orderer
Fabric-X committer / Notification Service
```

The orchestrator exposes Fabric `ProcessProposal` for custom query/invoke and
the standard Gateway `Evaluate`, `Endorse`, `Submit`, `CommitStatus`, and
`ChaincodeEvents` RPCs on the same mTLS gRPC endpoint. The helper
is embedded in the orchestrator process. The external chaincode remains a
separate CCAAS process.

## Gateway Evaluate

Gateway `Evaluate` uses the official `fabric-protos-go-apiv2/gateway` messages.
The Go Gateway SDK addresses a chaincode as `<name>:<namespace>`, for example
`GetContract("asset-transfer:1")`. Only the chaincode name field is split;
function names, named-contract prefixes and argument bytes are preserved.
Version is not part of this routing convention. The committed definition is
selected by channel and logical name, with its active version and sequence
passed to the helper and chaincode resolver.

Gateway and custom `ProcessProposal` share original proposal authentication,
MSP authorization and query execution. Gateway additionally requires its outer
channel and transaction ID to match the signed proposal. Signed header and
payload chaincode IDs must agree. The signature is checked over the original
bytes; parsing a qualified name never rewrites the signed proposal. Invocation
permissions apply to the parsed name and namespace. Legacy custom-operation and
namespace metadata do not override Gateway routing. Evaluation requires a
committed definition and rejects initialization or an unmet init gate.

The shared query execution uses the helper's invocation-local Query Service
view. Gateway returns `EvaluateResponse.Result` as the original `peer.Response`,
preserving binary payloads, rather than the custom invocation JSON wrapper.
Chaincode failures are gRPC errors with Gateway error details. Context
cancellation and the Orchestrator's request deadline bound evaluation.

Without explicit target organizations, evaluation stays local. If the SDK sets
`TargetOrganizations` (Go option `WithEndorsingOrganizations`), an eligible
local instance is preferred, otherwise existing remote discovery supplies
ordered candidates from the requested organizations. Each remote independently
authenticates and authorizes the original client proposal. Forwarded evaluation
is restricted to one hop to detect an endpoint that belongs to the wrong
organization. This restriction grants no permissions. Transport failures may
try another eligible candidate; signature, authorization, lifecycle and
chaincode errors are returned to the client. Discovery does not rank candidates
by ledger height or load.

Evaluation collects one result and never enters transaction-store
reconciliation, endorsement aggregation, ordering, or finality tracking.
Simulated writes, deletes and events do not change the ledger. Event subscribers
receive only events from transactions that Fabric-X has committed successfully.

## Gateway Endorse

Gateway `Endorse` shares proposal authentication and `<name>:<namespace>`
routing with Evaluate. It requires a committed logical chaincode definition,
resolves its active version and sequence, and checks the signed initialization
intent against the lifecycle init gate.

The shared `endorseInvocation` stage performs local execution, discovers remote
Orchestrators, authenticates their native endorsements, and compares their
responses, native transaction bytes, and lifecycle revisions. Custom Invoke
surrounds this stage with its existing duplicate detection and transaction
tracking, then submits and observes finality. Gateway Endorse returns the
prepared result without starting
submission tracking, reconciling older pending transactions, or waiting for
finality. Endorsing an Init transaction leaves the definition uninitialized.
Each helper closes its own Query Service view when its execution finishes.

Without explicit organizations, the existing namespace policy planner selects
and verifies sufficient distinct signing identities. The SDK option
`WithEndorsingOrganizations` requests one authenticated, matching endorsement
from each listed organization, in the supplied order. Duplicates are ignored;
an unavailable required organization fails endorsement. As with Fabric Gateway,
an explicit organization set replaces automatic policy planning, and the client
is responsible for requesting sufficient endorsements. Fabric-X enforces the
namespace policy when the native transaction is eventually validated. Local
execution remains the comparison reference even if the client requests only
remote signing organizations.

`EndorseResponse.PreparedTransaction` is an unsigned `common.Envelope` with the
Fabric transaction structure expected by Gateway SDKs:

| Field | Contents |
|---|---|
| `common.Payload.Header` | Original client channel and signature headers, preserving creator, nonce, timestamp, and transaction ID |
| `ChaincodeActionPayload.ChaincodeProposalPayload` | Original proposal input with its transient map removed |
| `ProposalResponsePayload.ProposalHash` | Hash of the headers and public proposal payload |
| `ChaincodeAction.Response` | Original chaincode status, message, and binary response payload |
| `ChaincodeAction.Results` | Complete native `applicationpb.Tx`, including its namespace endorsements |
| `ChaincodeAction.Events` | Simulation event derived from endorsed native metadata, when present |
| `ChaincodeEndorsedAction.Endorsements` | The selected native endorsement identities and signatures |

The native namespace, transaction ID, input arguments, and response payload are
checked against the prepared transaction during packaging. Native endorsement
signatures retain their Fabric-X signing format. The envelope provides SDK
parsing and client authorization; native transaction validation remains a
Fabric-X responsibility. The event in a prepared transaction is a simulation
result and does not imply commitment.

The helper also includes a version-one `GatewayProposalBinding` in native
`Tx.Metadata[1]` before signing the completed native contents. It contains the
public proposal hash, active definition version and sequence, and response
status and message. The proposal hash binds the original headers and public
input, including routing and Init intent, while excluding transient values.
All organizations independently construct this binding from the original
proposal and their active definition. `Metadata[0]` retains the SDK event,
response payload, and input arguments. The namespace signatures cover both
entries. The helper reuses the SDK's RW-set conversion and signs again after
adding this metadata because that SDK builder does not accept extra metadata.

The SDK extracts the result and signs the outer envelope payload before Submit.
The prepared format is self-contained and requires no retained helper view or
in-memory Endorse session. Prepared envelopes from the earlier implementation
without the signed native binding require a fresh Endorse call.

## Gateway Submit

Gateway `Submit` authenticates the client signature over the exact envelope
payload, checks its channel and transaction ID, and applies the configured
chaincode/namespace invocation permissions. It accepts exactly one prepared
action and verifies the public projection against the native contents. Headers,
arguments, response, events, endorsements, and native proposal binding must
agree. Re-signing an altered wrapper with the client's key cannot replace the
proposal hash or revision attested by the native endorsers.

Before a new submission, the current lifecycle definition must match the signed
revision and satisfy its Init gate. Submit verifies each namespace endorsement,
rejects duplicate signing identities, and enforces the current native namespace
policy, including when the client explicitly chose the endorsing organizations.
Invalid submissions do not reserve their transaction IDs in the local store.
The layer sends the same native transaction bytes that appeared in the
client-signed prepared envelope; it performs no chaincode execution or fresh
endorsement.

`submitTransaction` is the shared ordering stage used by both Gateway Submit and
custom Invoke. It uses an SDK connection and the native packager with a
request-scoped broadcast stream, checks the orderer's acknowledgment, and
updates the transaction store. This avoids the SDK sender's fire-and-forget
behavior. Custom Invoke establishes its finality subscription before this
stage, preserves its optional post-submit delay, and then waits for finality.
Gateway Submit returns an empty `SubmitResponse` after ordering acknowledges the
transaction. Commitment and validation can still fail later.

While a transaction remains pending in this instance, same-payload retries reuse
its submission receipt and a different payload with the same transaction ID is
rejected. Client signatures are rechecked on retries; different valid signatures
over identical payload bytes do not cause another send while that record exists. If a send may have reached ordering but its acknowledgment
was lost, the record stays `SUBMITTED` with an uncertain outcome. It is not
automatically resent. Later Invoke or Submit requests lazily reconcile pending
records through the Query Service in a bounded asynchronous pass, without
waiting for old work or returning older transactions in the current response.
There is no periodic tracker. Confirmed finality removes the application record;
any unfinished Init marker or mirror update has a separate small pending task.
Submit itself does not mark Init complete merely because ordering acknowledged it.

The store contains pending work only. It drops execution failures before submission
and confirmed final outcomes, retaining responses temporarily only through references
held by active callers. It keeps neither signed proposals nor full endorsements
after submission. Historical Invoke/Submit response replay and request-digest checks
are outside this pending-only tracker; clients can use CommitStatus to read an old
transaction's ledger outcome. A fresh transaction ID is a new request. Duplicate
protection does not extend past eviction, across restarts, or across independent
receiving instances.
Submitting a self-contained envelope to an instance that did not endorse it is
supported, provided it has the required trust, definition, and namespace policy.

Client-envelope authentication occurs at the compatibility boundary. The ordered
`MESSAGE` envelope contains the unchanged native transaction and its namespace
signatures. The outer Gateway client signature is not copied into that native
envelope; Fabric-X validates the native namespace endorsements and ledger
dependencies. Gateway `CommitStatus` observes the native validation result;
`ChaincodeEvents` reads events from the committed block store.

## Gateway CommitStatus

`CommitStatus` accepts the standard `SignedCommitStatusRequest`. It verifies
the signature over the exact serialized request with the existing trusted MSP
configuration, including certificate validity and revocation checks, and binds
the lookup to the configured channel. Status is channel-scoped: the signed
request contains a transaction ID and reader identity, without a chaincode or
namespace. By default, valid identities from the configured trusted MSPs may
read status. Set `proposal-auth.commit-status-policy`, for example
`"OR('org-0.client','org-1.client')"`, to restrict readers with a separate
signature policy. Invocation permissions remain separate.

The request follows this sequence:

1. Query the transaction's stored status. Return immediately if a final outcome
   exists, without opening a notification subscription.
2. Subscribe to notifications for that transaction ID if the outcome is unknown.
3. Query again to recover a commit between the first lookup and registration.
4. Wait for finality until the earlier of the client deadline and
   `request-timeout`.

The Notification Service does not acknowledge registration and does not replay
old events. During an active status request, a bounded status query runs once
per second, after stream failure or a subscription timeout, and shortly before
the request deadline. Each lookup has at most a one-second timeout within the
remaining request budget. These checks recover missed notifications and allow
status requests to continue if notifications are unavailable. The per-stream
subscription is bounded by `finality-timeout`; if it ends first, request-scoped
queries continue until the overall deadline. All subscriptions, receivers,
and timers stop when the status request ends. Custom Invoke's lazy store
maintenance remains unchanged.

The receiving Orchestrator needs no local endorsement or submission record,
so historical status works after a restart or through another instance with
the necessary trust and Query Service access. When a matching submitted local
record exists and is available, confirmed finality refreshes that record and
removes it after scheduling any unfinished Init bookkeeping. Status requests do
not execute or resubmit application transactions, create records for unrelated IDs, or reconcile
other transactions. A timeout or cancellation returns a gRPC error and leaves
the transaction's outcome unknown rather than marking it rejected.

Fabric-X and Fabric use different validation-code enums. The adapter returns
the native block number and maps outcomes by meaning:

| Fabric-X outcome | Gateway validation code |
|---|---|
| `COMMITTED` | `VALID` |
| `ABORTED_MVCC_CONFLICT` | `MVCC_READ_CONFLICT` |
| Invalid or missing namespace signatures, invalid namespace policy | `ENDORSEMENT_POLICY_FAILURE` |
| `REJECTED_DUPLICATE_TX_ID` | `DUPLICATE_TXID` |
| Malformed envelope or payload | `BAD_PAYLOAD` |
| Missing or conflicting transaction ID | `BAD_PROPOSAL_TXID` |
| Unsupported payload | `UNSUPPORTED_TX_PAYLOAD` |
| Malformed namespaces or keys | `BAD_RWSET` |
| Disallowed blind writes or no writes | `INVALID_WRITESET` |
| Invalid configuration transaction | `INVALID_CONFIG_TRANSACTION` |

Absent, unspecified, and unrecognized statuses are not treated as final.
Transaction-ID lookups follow Fabric-X's stored index; a later duplicate
occurrence does not replace the original indexed outcome. The ordinary Go
Gateway SDK `SubmitTransaction` helper now completes its Endorse, Submit, and
CommitStatus stages and reports validation failures as SDK commit errors.

## Gateway ChaincodeEvents

Chaincode `SetEvent` capture, custom Invoke responses, and Gateway event streams
share event decoding. The helper keeps SDK event name/payload metadata at
`applicationpb.Tx.Metadata[0]` and appends a `ChaincodeIdentity` record containing
type `fabric-x-chaincode-identity`, schema version `1`, channel, logical chaincode
name, and native namespace. Namespace signatures cover this record. Custom
invocations store it at index `1`; Gateway invocations keep the existing
version-one proposal binding at index `1` and put identity at index `2`.
Submit checks a present identity against the original signed proposal. Previously
prepared Gateway transactions without the extra record remain submit-compatible.

`chaincodeEventFromTransaction` decodes the name and binary payload once for
either API. The custom adapter preserves its existing namespace-valued
`chaincode_id` response field and attaches the event only after `COMMITTED`
finality. The Gateway adapter returns the requested qualified name, for example
`asset-transfer:1`, in `peer.ChaincodeEvent.ChaincodeId`.

Gateway `ChaincodeEvents` authenticates the exact signed subscription request
using the same MSP reader verification as CommitStatus and enforces the configured
channel and `<name>:<namespace>` routing. Valid members of trusted MSPs may
subscribe by default, independently of invocation permissions. Optional
`proposal-auth.chaincode-events-policy`, for example
`"OR('org-0.client','org-1.client')"`, restricts channel event readers.

Each subscriber uses the existing sidecar connection for block queries and a
signed committed-block delivery request. The sidecar's durable cursor provides
both history and new blocks; the Orchestrator does not need local endorsements,
submission records, or running CCaaS services to serve events. The stream supports:

- Omitted or `NextCommit` start positions: capture the current block-store height
  and open a specified cursor there, so commits during setup are replayed.
- `WithStartBlock(n)`: replay from block `n`, including zero, or wait for a future
  block. Earlier positions are skipped when following a future start.
- `WithCheckpoint`: locate the previous transaction's block through the stored
  transaction index, skip that event and earlier events, and resume within the
  same block before proceeding to later blocks.

These positions follow the [Fabric Gateway event protocol](https://github.com/hyperledger/fabric/blob/main/internal/pkg/gateway/chaincodeevents.go).
Only native transactions marked `COMMITTED` in the block validation filter are
eligible. The handler checks the signed channel/name/namespace identity, filters
by the requested chaincode, skips blocks without matching events, and sends one
response per matching block in transaction order. Rejected transactions never
produce stream events. Missing validation information or inconsistent identified
event metadata ends the stream with `DATA_LOSS`.

The client context owns the subscription, backpressure, and backend cursor.
`request-timeout` bounds setup queries but does not cap an established event
stream. Cancellation or disconnection closes its backend stream. Backend errors
end the subscription; applications can reconnect with their saved checkpoint.
There is no event polling worker or transaction-store maintenance triggered by
event reads.

Update participating Orchestrators before obtaining new endorsements so they
produce identical signed metadata. Historical records written without logical
chaincode identity are skipped: a shared namespace cannot safely identify their
emitter. A checkpoint naming such a record is rejected with
`FAILED_PRECONDITION`. This event API does not reconstruct information absent
from older blocks. The SDK's separate block/private-data event APIs use Fabric
peer Deliver services, which the Orchestrator does not expose.

## Invoke Flow

```text
client signed proposal
  -> orchestrator ProcessProposal
  -> original client signature and MSP trust verification
  -> normalize request
  -> authorize chaincode and native namespace
  -> lifecycle/init validation
  -> namespace policy lookup from Query Service
  -> local helper execution
  -> remote helper execution through remote orchestrators when policy requires it
  -> canonical result comparison and signer selection
  -> verify selected namespace endorsements
  -> Fabric-X endorsement merge
  -> orderer submit
  -> finality wait
  -> client JSON response
```

The original client creator, nonce, signed proposal, timestamp, transient data,
and chaincode name are preserved into the helper/shim path. Lifecycle selects the
active definition by channel and name; helper decorations use its committed
version and sequence without rewriting the signed client proposal.

Proposal admission and lifecycle RPCs share MSP verification through
`pkg/auth`; each keeps its own operation policies. Lifecycle configuration and
signed coordination are described in [lifecycle permissions](lifecycle.md#authentication-and-permissions).

## Proposal Authentication

Every public `ProcessProposal` and Gateway `Evaluate`/`Endorse` request is checked before lifecycle lookup,
execution, idempotency replay, or submission. This includes queries, invokes,
and execute-only endorsement requests forwarded by another organization.
The original client proposal is verified before the helper constructs its
internal execution proposal.

```yaml
proposal-auth:
  msps:
    - msp-id: org-0
      msp-dir: ../artifacts/peerOrganizations/peer-org-0/msp
    - msp-id: org-1
      msp-dir: ../artifacts/peerOrganizations/peer-org-1/msp
  rules:
    - chaincode: sample
      namespace: "1"
      policy: "OR('org-0.member','org-1.member')"
```

MSP directories contain public enrollment trust material: CA certificates,
intermediates, revocation lists, and MSP configuration, as applicable. These
are separate from the TLS CA roots used for network connections. Paths follow
the existing configuration convention and are relative to the process working
directory. Trust and policies are loaded at startup; restart the orchestrator
after changing them.

The verifier validates the creator through the configured Fabric-X MSP library,
checks the signing certificate's validity period at admission, and verifies the
signature over the unchanged original `ProposalBytes`. Proposal format, channel,
timestamp window, nonce, and transaction ID are also checked. The TLS caller
may be a remote orchestrator; authorization always uses the original client.

Rules grant access when both selectors match and the client's authenticated
identity satisfies the Fabric signature-policy expression. Selectors are exact
names or `"*"`; any matching grant can authorize the request. No matching grant
means access is denied. Rules apply to query, invoke, and endorsement requests
and check the effective Fabric-X namespace, including an explicit namespace
override. Use narrower selectors to restrict access to particular deployments.

The demo uses wildcard selectors and `.member` policies because its generated
MSPs disable Node OUs. Deployments with Node OUs enabled can use `.client`,
`.admin`, or other supported Fabric role principals. The policy is evaluated
against the single original client, separately from the multi-organization
endorsement policy governing the resulting Fabric-X transaction.

Missing or invalid trust/permission configuration prevents startup. Invalid
client identities or signatures return gRPC `Unauthenticated`; authenticated
clients without a matching permission return `PermissionDenied`. This
verification belongs to the proposal API; lifecycle RPCs retain their separate
signed-request verification.

## Helper State Semantics

The helper gives chaincode Fabric-like state behavior without writing committed
state directly.

```text
GetState(key)
  -> on the first read, begin a repeatable-read Query Service view
  -> read committed value through that invocation's view
  -> record the key's committed version or absence for MVCC validation

PutState(key, value)
  -> record pending write effect

DelState(key)
  -> record pending delete effect
```

`GetState` returns committed values even after the same invocation calls
`PutState` or `DelState` for that key, matching Fabric's simulation semantics.
Pending writes and deletions are retained for the transaction's write set;
Fabric-X validates the recorded read versions before committing those effects.

Each helper owns one view per invocation and opens it only when chaincode first
reads state. Subsequent reads, including reads of different keys, use that same
view while concurrent transactions continue committing. The helper closes the
view when execution ends, including on errors or cancellation; cleanup has a
separate two-second deadline. Invocations that never read state do not open
views. A mutex protects only the invocation's view and read/write bookkeeping;
it does not lock committed state or coordinate other invocations.

View creation and read failures abort execution, even if chaincode ignores the
read error. An expired view is never replaced with a newer snapshot mid-execution.
The Query Service caps view lifetime (`max-view-timeout`, currently 10 seconds in
the sample configuration). It can also aggregate nearby view handles onto the
same database snapshot, so a new handle does not select a historical block or
guarantee the newest network-wide block. Snapshot establishment and aggregation
remain Query Service responsibilities.

View IDs stay local to each helper. Organizations independently establish their
views; their Orchestrators compare the resulting responses and read/write sets
without exchanging view IDs. Different committed key versions can still prevent
endorsement aggregation, and MVCC can still reject a transaction whose read
dependencies changed before validation.

## Remote Endorsement

Namespace policies are evaluated against actual signing identities. For an MSP
policy, each endorsement's certificate is validated against the trusted MSPs in
`proposal-auth.msps`, its MSP is checked against the requested organization, and
its signature is verified over the native namespace transaction and metadata.
Invocation permissions in `proposal-auth.rules` remain separate from namespace
endorsement requirements.

```text
policy requirement: org-1
resolver result:    [org1-orchestrator-a, org1-orchestrator-b]
execution:          try candidates in order until a valid signature selection satisfies the policy
```

`AND`, `OR`, and `OutOf` rules may require distinct signers from the same MSP.
The collector continues through that organization's candidates when necessary;
duplicate certificates do not count as additional signers, even if returned by
different endpoints or serialized differently. Alternative organizations in an
`OR` or `OutOf` policy can be tried after a candidate fails.

Principal matching uses the configured MSP implementation, including roles,
organizational units, exact identities, combined principals, and nominal X.509
identities. Roles such as `.peer` and `.client` require the appropriate MSP Node
OU configuration. The sample network uses `.member` policies because its MSPs
disable Node OUs.

The selector assigns each signing identity at most once and considers alternate
assignments when broad and specific principals overlap. Nested policies can
require extra signatures and a different order under Fabric-X's evaluator; the
selector searches these alternatives within the request deadline. The final
selection, including its signature order, is checked with the same namespace signature
verifier used by Fabric-X before submission. Key-based threshold policies are
also verified against their configured key. A mismatch between executions
aborts the request; a policy without enough qualifying identities is not
submitted.

The Orchestrator and each execute-only remote resolve the current namespace
policy version independently and pass it to the helper's native endorsement
builder. Different versions therefore produce different transaction payloads
and cannot be merged. Fabric-X still performs final policy and MVCC validation.

Each Orchestrator signs using its configured `identity`. To provide two signers
from the same organization, configure separate instances with different signing
certificates and list their endpoints under that MSP using the existing static
or dynamic resolver. Additional same-organization endpoints are excluded from
cross-organization lifecycle RPCs.

## Result Matching

The orchestrator compares policy-relevant execution results before submission:

```text
status
message
chaincode response payload
Fabric-X transaction payload
```

If results differ, the transaction is not submitted.

## Finality

For invokes, the orchestrator subscribes to Notification Service finality before
submitting the merged endorsement and waits once within the request's deadline.

If the caller's finality wait expires after submission, the response contains
`submitted: true` and `finality_pending: true`, without a commit status or
committed event. The local transaction remains `SUBMITTED`; notification
timeouts, rejected subscriptions, query failures, and missing statuses do not
establish a ledger outcome. That client request then ends and its notification
subscription is canceled. That timeout starts no automatic checks.

A later Invoke, Gateway Submit, or definition commit can trigger one asynchronous
maintenance pass. New submissions never await that pass. At most one pass runs
per instance, with no queued reruns or periodic polling. Each pass checks at most
32 pending application transactions and advances at most 32 pending lifecycle
operations, using a total budget of at most five seconds (also bounded by
`finality-timeout`). The status batch has a one-second maximum. Service shutdown
cancels and joins the pass before closing its connections.

Maintenance skips application records owned by active finality waits. Missing
statuses and query failures leave them `SUBMITTED`; it never resends an uncertain
transaction or adds older outcomes to a new response. Repeated requests reuse
local information only while the pending record exists.

A confirmed validation result supplies the status and block location to existing
callers, then removes the record from the map. Only `COMMITTED` responses include
chaincode events and trigger initialization bookkeeping. `COMPLETED` and `FAILED`
are temporary states for response handoff, not retained history. A committed Init
can leave a small lifecycle task for a missing marker, unknown marker finality,
or outstanding organization mirrors. That task is removed when complete or
superseded by a newer definition. Neither pending store survives process restart;
startup still reconstructs definitions and initialized markers already on ledger.

Queries use the same helper execution path but do not submit to the orderer and
do not emit committed chaincode events.
