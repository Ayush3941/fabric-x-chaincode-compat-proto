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
  -> normalize request
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
