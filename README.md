# Fabric-X Chaincode Compatibility Project Workspace

This repository is a local real-network test environment for the Fabric
chaincode compatibility prototype. It runs a real Fabric-X ordering and
committer path, then executes an unchanged Go Fabric chaincode as an external
chaincode service through the prototype helper/coordinator.

The current V1 proof is deliberately narrow:

```text
client CLI
-> coordinator HTTP API
-> helper peer.Endorser.ProcessProposal service
-> external Fabric chaincode-as-a-service
-> Fabric shim messages: GET_STATE / PUT_STATE / DEL_STATE / COMPLETED
-> Fabric-X Query Service for committed reads
-> in-memory RW capture and read-your-writes overlay
-> Fabric-X SDK MESSAGE transaction
-> real Arma orderer
-> Fabric-X committer sidecar/coordinator/verifier/VC
-> Notification Service finality
-> block/query inspection
```

This is not a mock ledger. Blocks are committed under
`runtime/committer/ledger`, and the committed state is readable through the
Fabric-X Query Service.

## What V1 Proves

The current prototype demonstrates:

- one external Go chaincode service using the normal Fabric shim
- one helper connected to Fabric-X Query Service
- one coordinator that submits and waits for finality
- public point reads through `GetState`
- writes and deletes through `PutState` and `DelState`
- read-your-writes behavior inside one chaincode invocation
- `GetArgs`, `GetStringArgs`, `GetFunctionAndParameters`
- `GetTxID` and `GetChannelID`
- `CreateCompositeKey` and `SplitCompositeKey`
- `shim.Success`, `shim.Error`, `shim.OK`, and `shim.ERROR`
- one event payload through `SetEvent`
- real Fabric-X transaction submission and commit verification

Known V1 limitation: because the current Fabric-X SDK event field only carries
event payload bytes, the committed event name is the SDK default `log`, not the
original Fabric event name passed to `stub.SetEvent`.

## Layout

```text
chaincode_helper/          helper, coordinator, and client CLI
sample_external_chaincode/ unchanged-style Go chaincode-as-a-service sample
cmd/block-dump/            block inspection tool for committed Fabric-X blocks
cmd/rws-smoke/             lower-level SDK RW-set smoke client
committerconfig/           Fabric-X committer service configs
networkconfig/             crypto/configtx/Arma network inputs
ordererconfig/             Arma role config templates
fxconfig/                  namespace lifecycle client config templates
patches/                   build compatibility patch for orderer image
scripts/                   build/start/create-namespace/smoke/stop helpers
artifacts/                 generated crypto/config/tx artifacts; ignored
runtime/                   local process logs and committer ledger; ignored
storage/                   Arma orderer runtime storage; ignored
bin/                       generated tools; ignored
```

## Prerequisites

Required on the host:

```text
docker
go
git
make
nc
openssl
```

By default, `scripts/build-images.sh` clones the required Fabric-X source
repositories into `third_party/.build` and builds from pinned refs.

If you are actively developing against local Fabric-X checkouts, place them next
to this repository:

```text
../fabric-x
../fabric-x-orderer
../fabric-x-committer
```

Then build with:

```bash
USE_LOCAL_REPOS=1 ./scripts/build-images.sh
```

## Fresh Setup

From the repository root:

```bash
./scripts/build-images.sh
./scripts/generate-artifacts.sh
./scripts/start-network.sh
./scripts/create-namespace.sh
```

What those commands do:

- `build-images.sh` builds `cryptogen`, `configtxgen`, `fxconfig`, the Arma
  all-in-one orderer image, and the committer test-node image.
- `generate-artifacts.sh` creates crypto material, Arma configs, channel config,
  and clears old runtime state.
- `start-network.sh` starts the real Fabric-X orderer and committer containers.
- `create-namespace.sh` creates namespace `0` with the default policy
  `OR('org-0.member')`.

Optional lower-level sanity check, before involving chaincode:

```bash
./scripts/smoke.sh
```

That submits a hard-coded Fabric-X RW set through `cmd/rws-smoke`.

## Build V1 Prototype Binaries

```bash
cd chaincode_helper
go build -o bin/client ./cmd/client
go build -o bin/helper ./cmd/helper
go build -o bin/coordinator ./cmd/coordinator

cd ../sample_external_chaincode
go build -o bin/sample-chaincode ./cmd/server

cd ..
go build -o bin/block-dump ./cmd/block-dump
```

## Run Tests

Run tests against source packages, not `go test ./...` from the Project root
after the network has started. The running network creates Docker-owned
directories under `storage/`, and the Go tool recursively walks those
directories before applying package filtering.

Use:

```bash
go test ./cmd/...

cd chaincode_helper
go test ./...

cd ../sample_external_chaincode
go test ./...

cd ..
```

## Run The V1 Services

Use three terminals. Start each terminal from the repository root.

Terminal 1, external chaincode service:

```bash
cd sample_external_chaincode
./bin/sample-chaincode -ccid '0:sample' -address 127.0.0.1:9999
```

Terminal 2, helper:

```bash
cd chaincode_helper
./bin/helper -c sampleconfig/helper1.yaml
```

Terminal 3, coordinator:

```bash
cd chaincode_helper
./bin/coordinator -c sampleconfig/coordinator1.yaml
```

The configured endpoints are:

```text
chaincode service: 127.0.0.1:9999
helper service:    127.0.0.1:9001
coordinator API:   127.0.0.1:9101
committer sidecar: 127.0.0.1:4001
query service:     127.0.0.1:7001
orderer router:    127.0.0.1:6022
```

Health check:

```bash
curl -sS http://127.0.0.1:9101/healthz
```

Expected output:

```text
ok
```

## Run A Real V1 Chaincode Simulation

Use the coordinator-backed client config. The `invoke` command submits a real
Fabric-X transaction and waits for Notification Service finality.

Create two committed keys first:

```bash
cd chaincode_helper

FABRIC_LOGGING_SPEC=error ./bin/client invoke \
  -c sampleconfig/client-coordinator.yaml \
  '{"Function":"put","Args":["asset1","old-value"]}'

FABRIC_LOGGING_SPEC=error ./bin/client invoke \
  -c sampleconfig/client-coordinator.yaml \
  '{"Function":"put","Args":["asset-to-delete","delete-me"]}'
```

Run the full V1 compatibility path:

```bash
FABRIC_LOGGING_SPEC=error ./bin/client invoke \
  -c sampleconfig/client-coordinator.yaml \
  '{"Function":"compatv1","Args":["asset1","new-value","asset-to-delete"]}'
```

The response should be JSON with:

```text
"status": 200
"submitted": true
"commit_status": "COMMITTED"
"payload": "... old-value ..."
"chaincode_event": { "event_name": "log", ... }
```

The payload is the chaincode response. It includes the context and shim values
observed by the chaincode, including:

```text
args
string_args
function
parameters
tx_id
channel_id
old_value
after_put_value
delete_old_value
after_delete_value
composite_key
split_object_type
split_attributes
ok_status
error_status
```

Verify committed state:

```bash
FABRIC_LOGGING_SPEC=error ./bin/client query \
  -c sampleconfig/client-coordinator.yaml \
  '{"Function":"get","Args":["asset1"]}'

FABRIC_LOGGING_SPEC=error ./bin/client query \
  -c sampleconfig/client-coordinator.yaml \
  '{"Function":"get","Args":["asset-to-delete"]}'
```

Expected:

```text
new-value
```

The second query should print an empty payload because the key was deleted.

## Inspect Committed Blocks

Get current block height and dump recent blocks:

```bash
FABRIC_LOGGING_SPEC=error ./bin/block-dump -artifacts ./artifacts -from 0
```

Inspect one transaction by ID:

```bash
FABRIC_LOGGING_SPEC=error ./bin/block-dump -artifacts ./artifacts -txid <tx_id>
```

For a successful `compatv1` transaction, block output should show:

```text
Envelope type=MESSAGE/0 channel=channelqc4
data: applicationpb.Tx namespaces=1 endorsements=1 metadata=2
event ... name=log payload=...
read_write key="asset-to-delete" version=... value=<nil>
read_write key="asset1" version=... value="new-value"
```

The physical block ledger is stored on the host at:

```text
runtime/committer/ledger/chains/fabric-x-committer/blockfile_000000
```

That file is binary and may contain many blocks. Use `block-dump` for readable
inspection.

## Fast Restart

If the Fabric-X network and namespace are already running, only rebuild and
restart the three V1 processes:

```bash
cd chaincode_helper
go build -o bin/client ./cmd/client
go build -o bin/helper ./cmd/helper
go build -o bin/coordinator ./cmd/coordinator

cd ../sample_external_chaincode
go build -o bin/sample-chaincode ./cmd/server

cd ..
```

Then restart:

```bash
cd sample_external_chaincode
./bin/sample-chaincode -ccid '0:sample' -address 127.0.0.1:9999
```

```bash
cd chaincode_helper
./bin/helper -c sampleconfig/helper1.yaml
```

```bash
cd chaincode_helper
./bin/coordinator -c sampleconfig/coordinator1.yaml
```

## Stop And Clean

Stop Fabric-X containers but keep artifacts and ledger state:

```bash
./scripts/stop-network.sh
```

Full cleanup of generated artifacts and runtime state:

```bash
./scripts/clean.sh
```

## Current Scope

Included in V1:

- single Fabric-X namespace: `0`
- single organization policy by default: `OR('org-0.member')`
- one helper and one coordinator
- public point reads/writes/deletes
- one external Go chaincode service
- Query Service reads
- Notification Service finality
