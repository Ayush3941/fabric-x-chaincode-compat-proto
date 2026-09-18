# Fabric-X Chaincode Compatibility Service

This folder contains the compatibility service binaries:

- `cmd/orchestrator`: serves the client-facing ProcessProposal API, lifecycle API, embedded helper path, remote endorsement calls, Fabric-X submit, and finality wait.
- `cmd/client`: sends MSP-signed invoke/query proposals to the orchestrator.
- `pkg/helper`: executes one chaincode proposal, owns the per-invocation execution context, reads committed state through Query Service, and builds Fabric-X endorsement material.
- `pkg/shim`: speaks the Fabric chaincode-as-a-service shim protocol with the external chaincode process.
- `pkg/lifecycle`: packages, installs, approves, commits, and resolves CCAAS connection metadata in the orchestrator's in-memory lifecycle store.
- `pkg/orchestrator`: coordinates local helper execution, remote orchestrator execution, result matching, endorsement merge, orderer submit, and Notification Service finality.

Use the repository-root [README.md](../README.md) for the full runnable flow.
That guide starts the Fabric-X network, runs both org chaincode services, runs
both orchestrators, installs lifecycle packages, commits the sample definition,
invokes `compatv2`, verifies committed state, and dumps the generated block.

Current execution shape:

```text
client CLI
-> org0 orchestrator
   -> local helper -> org0 CCAAS
   -> remote org1 orchestrator -> org1 helper -> org1 CCAAS
   -> compare results
   -> merge endorsements
   -> submit to Fabric-X orderer
   -> wait for Notification Service finality
```

The lifecycle definition records chaincode `name`, `version`, `sequence`,
`init_required`, `initialized`, and each org's local package mapping.
Endorsement policy is not stored in lifecycle state; it is resolved from the
Fabric-X namespace through Query Service during transaction execution.
