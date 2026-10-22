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
  -> canonical result comparison
  -> Fabric-X endorsement merge
  -> orderer submit
  -> finality wait
  -> client JSON response
```

The original client creator, nonce, signed proposal, timestamp, transient data,
chaincode name, and chaincode version are preserved into the helper/shim path.

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
  -> check local overlay
  -> if absent, read committed value from Fabric-X Query Service

PutState(key, value)
  -> update local overlay
  -> record write effect

DelState(key)
  -> update local overlay
  -> record delete effect
```

The local overlay provides read-your-writes inside one chaincode invocation.
Fabric-X validation still checks the final read/write effects at commit time.

## Remote Endorsement

Namespace policy is evaluated at MSP level. If namespace `1` requires
`org-0` and `org-1`, the initiating orchestrator runs local execution and asks
the remote org's orchestrator for execute-only endorsement.

```text
policy requirement: org-1
resolver result:    [org1-orchestrator-a, org1-orchestrator-b]
execution:          try candidates in order until one endorsement succeeds
```

Only one successful endorsement per required MSP is used in the merged
Fabric-X transaction.

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
submitting the merged endorsement. The client response includes the final commit
status and block number when the notification arrives.

Queries use the same helper execution path but do not submit to the orderer and
do not emit committed chaincode events.
