# Fabric-X Chaincode Compatibility Prototype

This repository is a real-network V1 prototype for running an unchanged Go
Fabric chaincode against Fabric-X infrastructure.

The V1 flow is:

```text
client CLI
-> orchestrator gRPC endpoint
-> internal helper execution path
-> external Fabric chaincode-as-a-service
-> Fabric-X Query Service for reads
-> local read/write capture
-> Fabric-X transaction submit
-> Notification Service finality
-> block inspection
```

This is not a mock ledger. The orderer and committer containers create real
Fabric-X blocks under `runtime/committer/ledger`.

## What Works

- External Go chaincode using the normal Fabric shim server.
- `GetState`, `PutState`, `DelState`, and read-your-writes behavior.
- `GetArgs`, `GetStringArgs`, `GetFunctionAndParameters`.
- `GetTxID`, `GetChannelID`.
- `stub.GetCreator`, `cid.GetMSPID`, `cid.GetID`, `stub.GetBinding`,
  `stub.GetDecorations`, `stub.GetSignedProposal`.
- `CreateCompositeKey`, `SplitCompositeKey`.
- `shim.Success`, `shim.Error`, `shim.OK`, `shim.ERROR`.
- One event payload through `SetEvent`.
- Real Fabric-X transaction submission and committed-status confirmation.

Known V1 limitation: Fabric-X SDK event metadata currently keeps the event
payload, but the committed event name is the SDK default `log`.

## Repository Layout

```text
compatibility_service/       orchestrator, internal helper packages, client CLI
sample_external_chaincode/   sample external Go chaincode service
cmd/block-dump/              readable block inspection tool
cmd/rws-smoke/               lower-level Fabric-X RW-set smoke client
scripts/                     build, setup, start, stop helpers
fxconfig/                    namespace setup config template
networkconfig/               crypto and channel config inputs
committerconfig/             Fabric-X committer container configs
ordererconfig/               Arma orderer config templates
artifacts/                   generated crypto/config artifacts, ignored
runtime/                     logs and committer ledger, ignored
storage/                     Arma runtime storage, ignored
bin/                         generated Fabric-X tools, ignored
```

## Prerequisites

Install these on the host:

```text
docker
go
git
make
nc
openssl
```

By default, `scripts/build-images.sh` clones pinned Fabric-X sources into
`third_party/.build`. To build from sibling local checkouts instead, run it with
`USE_LOCAL_REPOS=1`.

## Fresh Setup

From the repository root:

```bash
./scripts/build-images.sh
./scripts/generate-artifacts.sh
./scripts/start-network.sh
./scripts/create-namespace.sh
```

The default namespace is `0` with policy `OR('org-0.member')`.

## Build Prototype Binaries

```bash
cd compatibility_service
go build -o bin/client ./cmd/client
go build -o bin/orchestrator ./cmd/orchestrator
cd ../sample_external_chaincode
go build -o bin/sample-chaincode ./cmd/server
cd ..
go build -o bin/block-dump ./cmd/block-dump
```

## Start Services

Use two service terminals from the repository root.

Terminal 1 starts the external chaincode service:

```bash
cd sample_external_chaincode
./bin/sample-chaincode -ccid '0:sample' -address 127.0.0.1:9999
```

Terminal 2 starts the compatibility service:

```bash
cd compatibility_service
./bin/orchestrator -c sampleconfig/orchestrator.yaml --log-level DEBUG
```

Terminal 2 prints the service logs. Look for loggers named `orchestrator`,
`helper`, and `shim`. Some `grpc` logs can also appear in the same terminal.

```text
orchestrator: client proposal, helper call, submit, finality
helper: proposal parse, execution result, Fabric-X endorsement
shim: CCAAS connect, REGISTER, TRANSACTION, GET_STATE, PUT_STATE, DEL_STATE
```

`sampleconfig/orchestrator.yaml` sets `request-timeout: 45s`. That is the full
Gateway-style request deadline around helper execution, submit, and finality.
It must be greater than or equal to `finality-timeout`.

Use Terminal 3 for the demo client commands in the next section. The JSON
response examples are printed by the client in Terminal 3. Keep client logging
quiet; `DEBUG` on the client mostly prints gRPC internals.

Important endpoints:

```text
9999  external chaincode service
9102  orchestrator, with internal helper execution
4001  Notification Service / committer sidecar
7001  Query Service
6022  Arma orderer router
```

## Run The Demo

Run these commands from Terminal 3.

Without transient data:

```bash
cd compatibility_service
FABRIC_LOGGING_SPEC=error ./bin/client invoke -c sampleconfig/client.yaml '{"Function":"compatv2","Args":["asset-v2","value-v2","asset-v2-delete"]}'
```

With optional transient data:

```bash
FABRIC_LOGGING_SPEC=error ./bin/client invoke -c sampleconfig/client.yaml '{"Function":"compatv2","Args":["asset-v2-transient","value-v2-transient","asset-v2-transient-delete"],"Transient":{"secret":"transient-value","purpose":"compatv2-test"}}'
```

The client prints a large JSON response in Terminal 3. Verify these fields:

```text
"status": 200
"submitted": true
"commit_status": "COMMITTED"
"client_msp_id": "org-0"
"creator_bytes": 800
"binding_bytes": 32
"decorations": { "compat.decorator": "orchestrator", ... }
"signed_proposal_present": true
"signed_proposal_bytes": ...
"signed_proposal_signature_bytes": ... non-zero
"tx_timestamp_rfc3339": "..."
"transient_count": ...
"client_id": "..."
"chaincode_event": { "event_name": "log", ... }
```

`compatv2` is self-contained. It seeds `old-value` and `delete-me` inside the
same chaincode invocation, so no setup `put` transactions are required. It also
checks the current client identity path with `stub.GetCreator()`,
`cid.GetMSPID(stub)`, `cid.GetID(stub)`, `stub.GetBinding()`, and
`stub.GetDecorations()`. It also verifies proposal-carried data through
`stub.GetSignedProposal()`, `stub.GetTransient()`, and `stub.GetTxTimestamp()`.

The idempotency key now includes the client transaction ID. Re-running the same
CLI command creates a fresh nonce and tx_id, so it is treated as a new
transaction. A duplicate delivery of the same signed proposal is replayed from
the orchestrator's in-memory idempotency store.

Verify final state:

```bash
FABRIC_LOGGING_SPEC=error ./bin/client query -c sampleconfig/client.yaml '{"Function":"get","Args":["asset-v2"]}'
FABRIC_LOGGING_SPEC=error ./bin/client query -c sampleconfig/client.yaml '{"Function":"get","Args":["asset-v2-delete"]}'
```

Expected:

```text
value-v2
```

The second query should print an empty payload because the key was deleted.

## Inspect Blocks

Dump all current blocks:

```bash
FABRIC_LOGGING_SPEC=error ./bin/block-dump -artifacts ./artifacts -from 0
```

Dump one transaction:

```bash
FABRIC_LOGGING_SPEC=error ./bin/block-dump -artifacts ./artifacts -txid <tx_id>
```

A successful `compatv2` block should show a Fabric-X `MESSAGE` envelope with an
`applicationpb.Tx`, one endorsement, event metadata, and writes for the selected
keys. For the no-transient example:

```text
asset-v2 -> "value-v2"
asset-v2-delete -> <nil>
```

## Tests

Run tests from package roots:

```bash
go test ./cmd/...

cd compatibility_service
go test ./...

cd ../sample_external_chaincode
go test ./...
```

Avoid `go test ./...` from the repository root after the network has started,
because Docker-owned files under `storage/` can confuse recursive package
discovery.

## Stop And Clean

Stop the Fabric-X containers:

```bash
./scripts/stop-network.sh
```

Stop local V1 processes:

```bash
pkill -f 'sample-chaincode'
pkill -f '/bin/orchestrator'
```

Delete generated artifacts and runtime state:

```bash
./scripts/clean.sh
```
