# Fabric-X Chaincode Compatibility Prototype

This repository runs unchanged Go Fabric chaincode as an external chaincode
service and commits the resulting state change through Fabric-X.

The default demo uses two organizations:

```text
client -> org0 orchestrator -> org0 CCAAS
                         |
                         -> org1 orchestrator -> org1 CCAAS
                         |
                         -> Fabric-X orderer -> committer -> finality
```

Committed blocks are stored under `runtime/committer/ledger` and can be
inspected with `bin/block-dump`.

## Capabilities

- External Go chaincode through `shim.ChaincodeServer`
- MSP namespace policies and Fabric-X threshold-rule namespace policies
- Two-org endorsement collection for namespace `1`
- Lifecycle package, install, approve, readiness, commit, query, and init gate
- Lifecycle commit markers written as Fabric-X transactions
- Committed lifecycle definitions load again after restart
- CCAAS and remote orchestrator addresses can come from the sample resolver
- `GetState`, `PutState`, `DelState`, read-your-writes
- `GetArgs`, `GetStringArgs`, `GetFunctionAndParameters`
- `GetTxID`, `GetChannelID`, `GetCreator`, `GetBinding`, `GetDecorations`
- `GetSignedProposal`, `GetTransient`, `GetTxTimestamp`
- `CreateCompositeKey`, `SplitCompositeKey`
- `shim.Success`, `shim.Error`, `shim.OK`, `shim.ERROR`
- One event payload through `SetEvent`

Known limitation: Fabric-X SDK event metadata keeps the event payload, but the
committed event name is currently the SDK default `log`.

## Repository Layout

```text
compatibility_service/       orchestrator, embedded helper path, client CLI
sample_external_chaincode/   external Go chaincode service
sample_external_resolver/    sample user-owned resolver service
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

## Prerequisites

```text
docker
go
git
make
nc
openssl
```

By default, `scripts/build-images.sh` clones pinned Fabric-X sources into
`third_party/.build`. Set `USE_LOCAL_REPOS=1` only when using sibling local
checkouts.

## Set Up The Network

Run from the repository root:

```bash
./scripts/build-images.sh
./scripts/generate-artifacts.sh
./scripts/start-network.sh
./scripts/create-namespace.sh
```

`create-namespace.sh` creates:

```text
namespace 0: OR('org-0.member')
namespace 1: AND('org-0.member','org-1.member')
namespace 2: threshold policy bound to the org0 client certificate
```

If namespaces already exist, the script skips them.

## Build Binaries

Run from the repository root:

```bash
cd compatibility_service
go build -o bin/client ./cmd/client
go build -o bin/orchestrator ./cmd/orchestrator
cd ../sample_external_chaincode
go build -o bin/sample-chaincode ./cmd/server
cd ../sample_external_resolver
go build -o bin/sample-resolver ./cmd/server
cd ..
go build -o bin/block-dump ./cmd/block-dump
```

## Start Services

Use five service terminals and one client terminal.
Run each terminal block from the repository root.

### Terminal 1: org0 CCAAS

```bash
cd sample_external_chaincode
./bin/sample-chaincode -ccid '0:sample' -address 127.0.0.1:9999
```

### Terminal 2: org1 CCAAS

```bash
cd sample_external_chaincode
./bin/sample-chaincode -ccid '0:sample' -address 127.0.0.1:10000
```

### Terminal 3: sample resolver

```bash
cd sample_external_resolver
./bin/sample-resolver -listen 127.0.0.1:9300 -tls-mode mtls -tls-cert ../artifacts/peerOrganizations/peer-org-0/peers/helper.peer-org-0/tls/server.crt -tls-key ../artifacts/peerOrganizations/peer-org-0/peers/helper.peer-org-0/tls/server.key -client-ca ../artifacts/peerOrganizations/peer-org-0/tlsca/tlsca.peer-org-0-cert.pem,../artifacts/peerOrganizations/peer-org-1/tlsca/tlsca.peer-org-1-cert.pem
```

### Terminal 4: org0 orchestrator

```bash
cd compatibility_service
./bin/orchestrator -c sampleconfig/orchestrator.yaml --log-level info:grpc=error
```

### Terminal 5: org1 orchestrator

```bash
cd compatibility_service
./bin/orchestrator -c sampleconfig/orchestrator1.yaml --log-level info:grpc=error
```

Useful ports:

```text
9999   org0 CCAAS
10000  org1 CCAAS
9300   sample resolver
9102   org0 orchestrator
9202   org1 orchestrator
4001   Notification Service / block query
7001   Query Service
6022   Arma router
```

## Run Lifecycle

Run from `compatibility_service` in Terminal 6.

### Package

```bash
export FABRIC_LOGGING_SPEC=error
cd compatibility_service
./bin/orchestrator lifecycle package --path ../sample_external_chaincode/cc_go/org0_sample --label org0_sample_1 --output ../sample_external_chaincode/cc_package/org0_sample/org0_sample.tgz
./bin/orchestrator lifecycle package --path ../sample_external_chaincode/cc_go/org1_sample --label org1_sample_1 --output ../sample_external_chaincode/cc_package/org1_sample/org1_sample.tgz
```

### Install

```bash
./bin/orchestrator lifecycle install -c sampleconfig/admin-org0.yaml ../sample_external_chaincode/cc_package/org0_sample/org0_sample.tgz
./bin/orchestrator lifecycle install -c sampleconfig/admin-org1.yaml ../sample_external_chaincode/cc_package/org1_sample/org1_sample.tgz
./bin/orchestrator lifecycle queryinstalled -c sampleconfig/admin-org0.yaml
./bin/orchestrator lifecycle queryinstalled -c sampleconfig/admin-org1.yaml
```

### Approve And Commit

```bash
ORG0_PKG=$(./bin/orchestrator lifecycle queryinstalled -c sampleconfig/admin-org0.yaml | awk -F= '/^package_id=/{print $2; exit}')
ORG1_PKG=$(./bin/orchestrator lifecycle queryinstalled -c sampleconfig/admin-org1.yaml | awk -F= '/^package_id=/{print $2; exit}')
./bin/orchestrator lifecycle approveformyorg -c sampleconfig/admin-org0.yaml -n sample -v 1.0 --sequence 1 --package-id "$ORG0_PKG"
./bin/orchestrator lifecycle approveformyorg -c sampleconfig/admin-org1.yaml -n sample -v 1.0 --sequence 1 --package-id "$ORG1_PKG"
./bin/orchestrator lifecycle checkcommitreadiness -c sampleconfig/admin-org0.yaml -n sample -v 1.0 --sequence 1
./bin/orchestrator lifecycle commit -c sampleconfig/admin-org0.yaml -n sample -v 1.0 --sequence 1
./bin/orchestrator lifecycle querycommitted -c sampleconfig/admin-org0.yaml -n sample -v 1.0
./bin/orchestrator lifecycle querycommitted -c sampleconfig/admin-org1.yaml -n sample -v 1.0
```

Expected commit output includes:

```text
ledger_status=COMMITTED
org-0=true
org-1=true
```

## Invoke Compatv2

Run from `compatibility_service` in Terminal 6.

```bash
mkdir -p ../runtime/compatibility_service
./bin/client invoke -c sampleconfig/client.yaml --namespace 1 -n sample -v 1.0 '{"Function":"compatv2","Args":["asset-multiorg-v2","value-multiorg-v2","asset-multiorg-v2-delete"]}' | tee ../runtime/compatibility_service/compatv2-result.json
```

Expected client output is larger JSON. Check these fields:

```json
{
  "status": 200,
  "submitted": true,
  "commit_status": "COMMITTED",
  "chaincode_event": {
    "event_name": "log"
  }
}
```

With transient data:

```bash
./bin/client invoke -c sampleconfig/client.yaml --namespace 1 -n sample -v 1.0 '{"Function":"compatv2","Args":["asset-v2-transient","value-v2-transient","asset-v2-transient-delete"],"Transient":{"secret":"transient-value","purpose":"compatv2-test"}}'
```

Against threshold namespace `2`:

```bash
./bin/client invoke -c sampleconfig/client.yaml --namespace 2 -n sample -v 1.0 '{"Function":"compatv2","Args":["asset-threshold-v2","value-threshold-v2","asset-threshold-v2-delete"]}' | tee ../runtime/compatibility_service/threshold-compatv2-result.json
```

## Init-Required Lifecycle

Use `--init-required` to block normal invoke/query until one init transaction
commits.

```bash
ORG0_PKG=$(./bin/orchestrator lifecycle queryinstalled -c sampleconfig/admin-org0.yaml | awk -F= '/^package_id=/{print $2; exit}')
ORG1_PKG=$(./bin/orchestrator lifecycle queryinstalled -c sampleconfig/admin-org1.yaml | awk -F= '/^package_id=/{print $2; exit}')
./bin/orchestrator lifecycle approveformyorg -c sampleconfig/admin-org0.yaml -n sample-init -v 1.0 --sequence 1 --package-id "$ORG0_PKG" --init-required
./bin/orchestrator lifecycle approveformyorg -c sampleconfig/admin-org1.yaml -n sample-init -v 1.0 --sequence 1 --package-id "$ORG1_PKG" --init-required
./bin/orchestrator lifecycle checkcommitreadiness -c sampleconfig/admin-org0.yaml -n sample-init -v 1.0 --sequence 1 --init-required
./bin/orchestrator lifecycle commit -c sampleconfig/admin-org0.yaml -n sample-init -v 1.0 --sequence 1 --init-required
./bin/client invoke -c sampleconfig/client.yaml --namespace 1 -n sample-init -v 1.0 --is-init '{"Function":"compatv2","Args":["asset-init-demo","value-init-demo","asset-init-demo-delete"]}'
./bin/client invoke -c sampleconfig/client.yaml --namespace 1 -n sample-init -v 1.0 '{"Function":"compatv2","Args":["asset-after-init","value-after-init","asset-after-init-delete"]}'
```

## Query State

Run from `compatibility_service` in Terminal 6.

```bash
./bin/client query -c sampleconfig/client.yaml --namespace 1 -n sample -v 1.0 '{"Function":"get","Args":["asset-multiorg-v2"]}'
```

Expected:

```text
value-multiorg-v2
```

```bash
./bin/client query -c sampleconfig/client.yaml --namespace 1 -n sample -v 1.0 '{"Function":"get","Args":["asset-multiorg-v2-delete"]}'
```

Expected: empty output.

## Inspect Blocks

Run from the repository root:

```bash
txid=$(python3 -c 'import json; print(json.load(open("runtime/compatibility_service/compatv2-result.json"))["tx_id"])')
./bin/block-dump -artifacts ./artifacts -txid "$txid"
```

Expected shape:

```text
Envelope type=MESSAGE/0 channel=channelqc4
data: applicationpb.Tx namespaces=1 endorsements=1 metadata=2
endorsements[0] signers=2
signer[0] msp_id=org-0
signer[1] msp_id=org-1
```

## Service Logs

The orchestrator terminals show the execution path:

```text
[orchestrator] ProcessProposal -> tx=... namespace=1 chaincode=sample:1.0 fn=compatv2
[shim] Execute -> tx=... ccaas connect endpoint=127.0.0.1:9999
[helper] ProcessProposal -> tx=... chaincode execution completed status=200
[orchestrator] submitAndWaitFinality -> tx=... finality status=COMMITTED block=...
```

Terminal 6 prints the client JSON response. Terminals 4 and 5 print these
service logs.

## Test

Run unit tests:

```bash
cd compatibility_service
go test ./pkg/lifecycle ./pkg/orchestrator ./pkg/shim
```

Run real-network integration tests after setup:

```bash
cd compatibility_service
go test -tags=e2e ./integration/e2e -count=1 -v
```

Avoid `go test ./...` from the repository root after the network has started;
Docker-owned files under `storage/` can interfere with package discovery.

## Stop And Clean Up

Run from the repository root:

```bash
./scripts/stop-network.sh
pkill -f 'sample-chaincode'
pkill -f '/bin/orchestrator'
pkill -f '/bin/sample-resolver'
```

Reset only ledger and orderer storage:

```bash
./scripts/reset-ledger.sh
./scripts/start-network.sh
./scripts/create-namespace.sh
```

Full cleanup:

```bash
./scripts/clean.sh
```

## Notes

- Lifecycle installed packages are kept in memory inside each orchestrator.
- Committed lifecycle definitions are written to Fabric-X ledger namespace `0`.
- CCAAS and remote orchestrator endpoints are resolved lazily by static config
  or the sample resolver.
- The sample resolver is not part of the compatibility service runtime; users
  can replace it with their own resolver implementation.
- Current lifecycle identity is `name:version`; traditional Fabric treats
  `sequence` as the upgrade counter for a chaincode name.
