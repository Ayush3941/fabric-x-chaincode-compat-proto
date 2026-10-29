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

The orchestrator exposes a Fabric `ProcessProposal`-compatible gRPC API to the
client CLI. The helper is embedded in the orchestrator process. The external
chaincode remains a separate CCAAS process.

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
chaincode name, and chaincode version are preserved into the helper/shim path.

Proposal admission and lifecycle RPCs share MSP verification through
`pkg/auth`; each keeps its own operation policies. Lifecycle configuration and
signed coordination are described in [lifecycle permissions](lifecycle.md#authentication-and-permissions).

## Proposal Authentication

Every public `ProcessProposal` request is checked before lifecycle lookup,
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
subscription is canceled. There are no background trackers or periodic checks.

When a later invoke arrives, the Orchestrator makes one batched Query Service
request for older pending transactions and refreshes its internal store using
confirmed validation results. It skips transactions whose original requests are
still waiting for finality. Missing statuses and query failures leave records
`SUBMITTED`. This maintenance never resubmits transactions or adds older results
to the new invocation's response; the client receives only its requested
transaction. Explicitly retrying the same signed proposal reuses that
transaction's stored result without executing or submitting again.

A confirmed validation result moves the record to `COMPLETED` and supplies the
commit status and block location. Only `COMMITTED` responses include chaincode
events and trigger init finalization. These records remain instance-local and
in memory; recovery across restarts or other Orchestrators is not provided.

Queries use the same helper execution path but do not submit to the orderer and
do not emit committed chaincode events.
