# Fabric-X Chaincode Helper

This folder contains the V1 chaincode compatibility helper, orchestrator, and
test client. It is based on the Fabric-X custom endorser shape, but the executor
now talks to an external Fabric chaincode-as-a-service process through the real
Fabric shim message protocol.

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
-> helper peer.Endorser.ProcessProposal service
-> pkg/api ExecutionContext
-> pkg/shim CCAAS connector and message handler
-> external chaincode Invoke
-> GET_STATE routed to Fabric-X Query Service
-> PUT_STATE / DEL_STATE captured in memory
-> Fabric-X endorsement response
-> orchestrator submits to orderer and waits for Notification Service finality
```

## Layout

```text
cmd/helper/        helper service exposing peer.Endorser.ProcessProposal
cmd/orchestrator/   client-facing orchestrator and Fabric-X submit/finality path
cmd/client/        small CLI for query/invoke through the orchestrator
pkg/api/           ProcessProposal service, ExecutionContext, Query adapter
pkg/config/        YAML config structures
pkg/orchestrator/   gRPC ProcessProposal, helper call, submitter, notification wait
pkg/shim/          CCAAS connector and Fabric ChaincodeMessage handler
sampleconfig/      configs wired to ../artifacts from the Project network
```

## Build

Commands in this file assume your shell starts from the repository root.

```bash
cd chaincode_helper
go build -o bin/client ./cmd/client
go build -o bin/helper ./cmd/helper
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

Start the helper:

```bash
cd chaincode_helper
./bin/helper -c sampleconfig/helper.yaml
```

Start the orchestrator:

```bash
cd chaincode_helper
./bin/orchestrator -c sampleconfig/orchestrator.yaml
```

For proof-oriented logs during a demo, run helper and orchestrator with debug
logging:

```bash
./bin/helper -c sampleconfig/helper.yaml --log-level DEBUG
./bin/orchestrator -c sampleconfig/orchestrator.yaml --log-level DEBUG
```

At INFO level the logs show proposal receipt, helper execution, Fabric-X
submission, and finality. At DEBUG level they also show the CCAAS shim
GET_STATE, PUT_STATE, DEL_STATE, query view, and notification subscription
steps.

Submit a real V1 invoke:

```bash
cd chaincode_helper

FABRIC_LOGGING_SPEC=error ./bin/client invoke \
  -c sampleconfig/client.yaml \
  '{"Function":"compatv1","Args":["asset1","new-value","asset-to-delete"]}'
```

`compatv1` seeds the temporary old and delete values inside the same
invocation, so no setup `put` transactions are required.

The final response should include `commit_status: "COMMITTED"`.

## Important Files

- `pkg/api/service.go`
  - registers `peer.EndorserServer`
  - parses signed proposals
  - owns `ExecutionContext`
  - reads state through Query Service
  - builds Fabric-X endorsements

- `cmd/helper/executor.go`
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
  - calls the helper
  - submits Fabric-X transactions
  - waits on Notification Service and returns finality

## V1 Scope

Implemented for the sample chaincode:

- `GetState`, `PutState`, `DelState`
- read-your-writes overlay
- `GetArgs`, `GetStringArgs`, `GetFunctionAndParameters`
- `GetTxID`, `GetChannelID`
- `CreateCompositeKey`, `SplitCompositeKey`
- `shim.Success`, `shim.Error`, `shim.OK`, `shim.ERROR`
- event payload propagation through the current SDK `Event []byte` path

Deferred:

- multi-organization helper coordination
- range/rich/history queries
- private data
- cross-chaincode invocation
- preserving the original Fabric event name instead of SDK default `log`
