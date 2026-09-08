# Fabric-X Chaincode Compatibility Prototype

Prototype for running unchanged Go Fabric chaincode against Fabric-X.

CCAAS means Fabric chaincode running as an external service. A Fabric-X
namespace is the state and policy target used by the demo transaction. The
orchestrator accepts the client proposal, runs chaincode through the helper
path, collects required org endorsements, submits, and waits for finality.

Current flow:

```text
client CLI -> org0 orchestrator -> org0 CCAAS
                         |
                         -> org1 orchestrator -> org1 CCAAS
                         |
                         -> merge matching endorsements -> orderer -> committer -> notification finality
```

Committed blocks are stored under `runtime/committer/ledger`
and can be inspected with `bin/block-dump`.

## What Works

- External Go chaincode through `shim.ChaincodeServer`.
- Two organization static endorsement path: `org-0` and `org-1`.
- Namespace policy lookup from Fabric-X Query Service.
- Remote orchestrator execution when policy needs another MSP.
- Result matching before submit.
- Merged Fabric-X SDK endorsement responses.
- Fabric-X orderer submission and Notification Service finality.
- `GetState`, `PutState`, `DelState`, read-your-writes.
- `GetArgs`, `GetStringArgs`, `GetFunctionAndParameters`.
- `GetTxID`, `GetChannelID`, `GetCreator`, `GetBinding`, `GetDecorations`.
- `GetSignedProposal`, `GetTransient`, `GetTxTimestamp`.
- `CreateCompositeKey`, `SplitCompositeKey`.
- `shim.Success`, `shim.Error`, `shim.OK`, `shim.ERROR`.
- One event payload through `SetEvent`.

Known limitation: Fabric-X SDK event metadata keeps the payload, but the
committed event name is currently the SDK default `log`.

## Layout

```text
compatibility_service/       orchestrator, embedded helper path, client CLI
sample_external_chaincode/   external Go chaincode service
cmd/block-dump/              readable committed block dump
cmd/rws-smoke/               low-level Fabric-X RW-set smoke client
scripts/                     build, setup, start, stop helpers
fxconfig/                    namespace setup configs
networkconfig/               crypto and channel config inputs
committerconfig/             Fabric-X committer configs
ordererconfig/               Arma orderer configs
artifacts/                   generated crypto/config artifacts, ignored
runtime/                     logs and committer ledger, ignored
storage/                     Arma runtime storage, ignored
bin/                         generated Fabric-X tools, ignored
```

## Requirements

```text
docker
go
git
make
nc
openssl
```

By default `scripts/build-images.sh` clones pinned Fabric-X sources into
`third_party/.build`. Use `USE_LOCAL_REPOS=1` only if you want sibling local
checkouts.

## Fabric-X Code Used

- `scripts/build-images.sh` uses pinned `fabric-x`, `fabric-x-orderer`, and
  `fabric-x-committer` sources under `third_party/.build`.
- `scripts/create-namespace.sh` uses the generated `bin/fxconfig` tool and the
  configs under `fxconfig/`.
- Runtime endorsement, submit, network, and identity helpers come from
  `github.com/hyperledger/fabric-x-sdk`.
- Fabric-X protobufs and block helpers come from
  `github.com/hyperledger/fabric-x-common`.

## Fresh Setup

Run from the repository root:

```bash
./scripts/build-images.sh
./scripts/generate-artifacts.sh
./scripts/start-network.sh
./scripts/create-namespace.sh
```

The last command creates namespace `0` with `OR('org-0.member')` and namespace
`1` with `AND('org-0.member','org-1.member')`.

The namespace setup transactions are signed by both app orgs. That is separate
from the policy stored inside each namespace.

## Build

Run from the repository root:

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

Use four service terminals and one client terminal.

The first two terminals run the external chaincode services. The next two run
the org orchestrators and show service logs. Terminal 5 is the client terminal;
it runs the demo commands and prints the JSON response.

Service terminal 1, org0 chaincode:

```bash
cd sample_external_chaincode
./bin/sample-chaincode -ccid '0:sample' -address 127.0.0.1:9999
```

Service terminal 2, org1 chaincode:

```bash
cd sample_external_chaincode
./bin/sample-chaincode -ccid '0:sample' -address 127.0.0.1:10000
```

Service terminal 3, org0 orchestrator:

```bash
cd compatibility_service
./bin/orchestrator -c sampleconfig/orchestrator.yaml --log-level DEBUG
```

Service terminal 4, org1 orchestrator:

```bash
cd compatibility_service
./bin/orchestrator -c sampleconfig/orchestrator1.yaml --log-level DEBUG
```

The orchestrator terminals show `[orchestrator]`, `[helper]`, `[shim]`, and
`[grpc]` logs.

Example service log lines:

```text
[orchestrator] ProcessProposal -> tx=... operation=invoke channel=channelqc4 namespace=1 fn=compatv2 args=3
[helper] ProcessProposal -> tx=... chaincode execution completed status=200 reads=2 writes=2
[shim] sendTransaction -> tx=... shim TRANSACTION sent ccid=0:sample fn=compatv2 args=3
[grpc] AddTraceEvent -> [core] [Channel #...] Channel Connectivity change to READY
[orchestrator] submitAndWaitFinality -> tx=... finality status=COMMITTED block=3 txnum=0
```

Useful ports:

```text
9999   org0 chaincode service
10000  org1 chaincode service
9102   org0 orchestrator
9202   org1 orchestrator
4001   Notification Service / block query
7001   Query Service
6022   Arma orderer router
```

## Run Compatv2

Run from `compatibility_service` in Terminal 5:

```bash
mkdir -p ../runtime/compatibility_service
FABRIC_LOGGING_SPEC=error ./bin/client invoke -c sampleconfig/client.yaml '{"Function":"compatv2","Args":["asset-multiorg-v2","value-multiorg-v2","asset-multiorg-v2-delete"]}' | tee ../runtime/compatibility_service/compatv2-result.json
```

With transient data:

```bash
FABRIC_LOGGING_SPEC=error ./bin/client invoke -c sampleconfig/client.yaml '{"Function":"compatv2","Args":["asset-v2-transient","value-v2-transient","asset-v2-transient-delete"],"Transient":{"secret":"transient-value","purpose":"compatv2-test"}}'
```

`compatv2` is implemented in
`sample_external_chaincode/cmd/server/main.go`. It is normal Fabric chaincode
code. It checks state operations, context APIs, transient data, timestamp,
signed proposal, composite keys, and event payload while using the same
chaincode service path as the rest of the prototype.

The client terminal prints JSON like this:

```json
{
  "tx_id": "...",
  "status": 200,
  "payload": "...",
  "payload_base64": "...",
  "submitted": true,
  "commit_status": "COMMITTED",
  "block_num": 3,
  "idempotency_key": "...",
  "chaincode_event": {
    "event_name": "log",
    "payload": "..."
  }
}
```

Important values inside the returned `payload` string:

```text
client_msp_id: org-0
binding_bytes: 32
signed_proposal_present: true
signed_proposal_signature_bytes: non-zero
transient_count: 0, or 2 when using the transient example
```

Repeated in-flight requests with the same transaction context wait on the
stored result instead of running twice. The service log shows this duplicate
request handling, also called idempotency:

```text
[orchestrator] Execute -> idempotency_key=... duplicate request detected; waiting for stored result
```

## Verify State

Run from `compatibility_service` in Terminal 5:

```bash
FABRIC_LOGGING_SPEC=error ./bin/client query -c sampleconfig/client.yaml '{"Function":"get","Args":["asset-multiorg-v2"]}'
```

Expected:

```text
value-multiorg-v2
```

Run:

```bash
FABRIC_LOGGING_SPEC=error ./bin/client query -c sampleconfig/client.yaml '{"Function":"get","Args":["asset-multiorg-v2-delete"]}'
```

Expected: empty output.

## Inspect Block

Run from the repository root:

```bash
txid=$(python3 -c 'import json; print(json.load(open("runtime/compatibility_service/compatv2-result.json"))["tx_id"])')
FABRIC_LOGGING_SPEC=error ./bin/block-dump -artifacts ./artifacts -txid "$txid"
```

Without the saved result file, copy the `tx_id` printed by the client and run:

```bash
FABRIC_LOGGING_SPEC=error ./bin/block-dump -artifacts ./artifacts -txid <tx_id>
```

Expected block shape:

```text
Envelope type=MESSAGE/0 channel=channelqc4
data: applicationpb.Tx namespaces=1 endorsements=1 metadata=2
endorsements[0] signers=2
signer[0] msp_id=org-0
signer[1] msp_id=org-1
read_write key="asset-multiorg-v2" value="value-multiorg-v2"
read_write key="asset-multiorg-v2-delete" value=<nil>
```

`endorsements=1` means one endorsement bucket for one namespace. The signer
lines show the actual two organization signatures.

## Tests

Run unit tests from the repository root:

```bash
cd compatibility_service
go test ./pkg/orchestrator ./pkg/shim
cd ..
```

Run the real-network integration tests after Fresh Setup:

```bash
cd compatibility_service
FABRIC_LOGGING_SPEC=error go test -tags=e2e ./integration/e2e -count=1 -v
cd ..
```

The integration tests build `sample_external_chaincode/cmd/e2e-server`, start
temporary chaincode/orchestrator processes on high local ports, and cover
duplicate request handling, remote org unavailable, mismatched org result,
timeout before submit, retry after completed result, and same `tx_id` with
changed request conflict. Test logs are written under
`runtime/compatibility_service/e2e`.

Avoid `go test ./...` from the repository root after the network has started,
because Docker-owned files under `storage/` can interfere with recursive
package discovery.

## Stop

Run from the repository root after the demo:

```bash
./scripts/stop-network.sh
pkill -f 'sample-chaincode'
pkill -f '/bin/orchestrator'
```

Full cleanup:

```bash
./scripts/clean.sh
```

This removes generated artifacts, local runtime state, service binaries, and
local build caches. Run the fresh setup and build commands again after it.
