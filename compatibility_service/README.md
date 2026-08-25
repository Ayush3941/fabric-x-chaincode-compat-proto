# Fabric-X Chaincode Compatibility Service

This folder contains the V1 chaincode compatibility orchestrator and test
client. The orchestrator embeds the helper execution path in the same process.
That internal helper is based on the Fabric-X custom endorser shape, but the
executor talks to an external Fabric chaincode-as-a-service process through the
real Fabric shim message protocol.

The helper is intentionally stateless:

- it does not maintain a Fabric peer ledger
- it does not run a local world-state database
- it reads committed state through Fabric-X Query Service
- it captures reads, writes, deletes, responses, and event payloads per
  invocation
- it returns Fabric-X-format endorsements through `fabric-x-sdk`

Current V1 flow:

```text
client CLI
-> orchestrator Fabric-X SDK ProcessProposal gRPC API
-> internal helper ProcessProposal path
-> pkg/helper ExecutionContext
-> pkg/shim CCAAS connector and message handler
-> external chaincode Invoke
-> GET_STATE routed to Fabric-X Query Service
-> PUT_STATE / DEL_STATE captured in memory
-> Fabric-X endorsement response
-> orchestrator submits to orderer and waits for Notification Service finality
```

## Layout

```text
cmd/orchestrator/   client-facing orchestrator and Fabric-X submit/finality path
cmd/client/         small CLI for query/invoke through the orchestrator
pkg/helper/         ProcessProposal service, ExecutionContext, Query adapter
pkg/config/         YAML config structures
pkg/orchestrator/   gRPC ProcessProposal, internal helper call, submitter, notification wait
pkg/shim/           CCAAS connector and Fabric ChaincodeMessage handler
sampleconfig/       configs wired to ../artifacts from the Project network
```

## Build

Commands in this file assume your shell starts from the repository root.

```bash
cd compatibility_service
go build -o bin/client ./cmd/client
go build -o bin/orchestrator ./cmd/orchestrator
```

## Run

Start the Project Fabric-X network and namespace first:

```bash
./scripts/start-network.sh
./scripts/create-namespace.sh
```

Start the external chaincode service:

```bash
cd sample_external_chaincode
go build -o bin/sample-chaincode ./cmd/server
./bin/sample-chaincode -ccid '0:sample' -address 127.0.0.1:9999
```

Start the orchestrator in the foreground:

```bash
cd compatibility_service
./bin/orchestrator -c sampleconfig/orchestrator.yaml --log-level DEBUG
```

This terminal shows the service-side proof trace:

```text
[orchestrator] client proposal, helper call, submit, finality
[helper] proposal parse, execution result, Fabric-X endorsement
[shim] CCAAS connect, REGISTER, TRANSACTION, GET_STATE, PUT_STATE, DEL_STATE
```

`sampleconfig/orchestrator.yaml` sets `request-timeout: 45s`. That deadline
covers the whole orchestrator request and must be greater than or equal to
`finality-timeout`.

At INFO level the logs show proposal receipt, internal helper execution,
Fabric-X submission, and finality. At DEBUG level they also show the CCAAS shim
GET_STATE, PUT_STATE, DEL_STATE, query view, and notification subscription
steps. Run the client from another terminal with `FABRIC_LOGGING_SPEC=error`;
client-side debug output is mostly gRPC internals.

Submit a real compatibility invoke:

```bash
cd compatibility_service

FABRIC_LOGGING_SPEC=error ./bin/client invoke \
  -c sampleconfig/client.yaml \
  '{"Function":"compatv2","Args":["asset-v2","value-v2","asset-v2-delete"]}'
```

`compatv2` seeds the temporary old and delete values inside the same
invocation, so no setup `put` transactions are required. It also checks
`stub.GetCreator()`, `cid.GetMSPID(stub)`, `cid.GetID(stub)`,
`stub.GetBinding()`, and `stub.GetDecorations()`.

The final response should include `commit_status: "COMMITTED"`,
`client_msp_id: "org-0"`, `binding_bytes: 32`, populated compatibility
decorations, and non-empty creator/client identity fields.

To check the current idempotency prototype, run the same `compatv2` invoke
twice:

```bash
FABRIC_LOGGING_SPEC=error ./bin/client invoke \
  -c sampleconfig/client.yaml \
  '{"Function":"compatv2","Args":["asset-v2","value-v2","asset-v2-delete"]}'

FABRIC_LOGGING_SPEC=error ./bin/client invoke \
  -c sampleconfig/client.yaml \
  '{"Function":"compatv2","Args":["asset-v2","value-v2","asset-v2-delete"]}'
```

The second response should reuse the same `tx_id` and `block_num` and include
`"idempotent_replay": true`. The current store is in-memory and is reset when
the orchestrator restarts.

## Important Files

- `pkg/helper/service.go`
  - registers `peer.EndorserServer`
  - parses signed proposals
  - owns `ExecutionContext`
  - reads state through Query Service
  - builds Fabric-X endorsements

- `pkg/helper/executor.go`
  - adapts `endorsement.Invocation` to `pkg/shim.Invocation`
  - converts the shim bridge result into `endorsement.ExecutionResult`

- `pkg/shim/connector.go`
  - opens the `peer.Chaincode/Connect` stream to the external chaincode service
  - handles the initial `REGISTER` / `REGISTERED` / `READY` handshake
  - defines the state interface satisfied by `ExecutionContext`

- `pkg/shim/handler.go`
  - sends `TRANSACTION`
  - routes `GET_STATE`, `PUT_STATE`, and `DEL_STATE`
  - returns the chaincode `COMPLETED` response and event payload

- `pkg/orchestrator/service.go`
  - accepts Fabric-X SDK signed proposals over gRPC
  - calls the in-process helper path
  - submits Fabric-X transactions
  - waits on Notification Service and returns finality

## V1 Scope

Implemented for the sample chaincode:

- `GetState`, `PutState`, `DelState`
- read-your-writes overlay
- `GetArgs`, `GetStringArgs`, `GetFunctionAndParameters`
- `GetTxID`, `GetChannelID`
- `GetCreator`, `GetBinding`, `GetDecorations`
- `CreateCompositeKey`, `SplitCompositeKey`
- `shim.Success`, `shim.Error`, `shim.OK`, `shim.ERROR`
- event payload propagation through the current SDK `Event []byte` path

Deferred:

- multi-organization helper coordination
- range/rich/history queries
- private data
- cross-chaincode invocation
- preserving the original Fabric event name instead of SDK default `log`
