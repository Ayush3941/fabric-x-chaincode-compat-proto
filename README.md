# Fabric-X Chaincode Compatibility Prototype

This repository runs unchanged Go Fabric chaincode as an external chaincode
service and commits the resulting state change through Fabric-X.

The default demo starts two application organizations, installs one chaincode
definition, collects endorsements from both organizations, submits one Fabric-X
transaction, waits for finality, and dumps the committed block.

```text
client
  -> org0 orchestrator -> org0 CCAAS
  -> org1 orchestrator -> org1 CCAAS
  -> merge matching endorsements
  -> Fabric-X orderer -> committer -> finality
```

Committed blocks are stored under `runtime/committer/ledger` and can be
inspected with `bin/block-dump`.

## Capabilities

- External Go chaincode through `shim.ChaincodeServer`
- MSP namespace policies and Fabric-X threshold-rule namespace policies
- Policy-aware selection of distinct signing identities and signature verification before submission
- Two-organization endorsement for namespace `1`
- Original client proposal signature/MSP verification and invocation permissions
- Fabric Gateway SDK Evaluate, Endorse, Submit, CommitStatus, and ChaincodeEvents using `<name>:<namespace>` routing
- Lifecycle folder-based install, approve, readiness, commit, query, and init gate
- Lifecycle commit markers written as Fabric-X transactions
- Committed lifecycle definitions reloaded from the ledger after restart
- Static or resolver-based CCAAS endpoint lookup
- Static or resolver-based remote orchestrator lookup
- Multiple orchestrator candidates per organization
- `GetState`, `PutState`, and `DelState` with committed-state reads
- `GetArgs`, `GetStringArgs`, `GetFunctionAndParameters`
- `GetTxID`, `GetChannelID`, `GetCreator`, `GetBinding`, `GetDecorations`
- `GetSignedProposal`, `GetTransient`, `GetTxTimestamp`
- `CreateCompositeKey`, `SplitCompositeKey`
- `shim.Success`, `shim.Error`, `shim.OK`, `shim.ERROR`
- Named chaincode event through `SetEvent`
- Fabric Samples Asset Transfer Basic through the Fabric Contract API and CCAAS

## Layout

```text
compatibility_service/       orchestrator, embedded helper path, lifecycle, client CLI
sample_external_chaincode/   sample external Go chaincode service
sample_external_resolver/    sample user-owned resolver service
cmd/block-dump/              readable committed block dump
cmd/rws-smoke/               low-level Fabric-X RW-set smoke client
scripts/                     build, setup, start, stop, reset helpers
fxconfig/                    namespace setup configs for fxconfig
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

By default, `scripts/build-images.sh` clones pinned Fabric-X sources into
`third_party/.build`. Set `USE_LOCAL_REPOS=1` only when using sibling local
checkouts.

The default local Docker images are
`hyperledger/arma-4p1s:v1.0.3-project` and
`hyperledger/committer-test-node:v1.0.4-project`, matching the pinned Orderer
and Committer releases. The versions are visible in `docker ps`; `-project`
identifies locally built images. If you override the source refs, also set
`ORDERER_IMAGE` and `COMMITTER_IMAGE` to matching image tags when building and
starting the network.

## 1. Start Fabric-X

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

If the namespaces already exist, the script skips them.

## 2. Build Binaries

Run from the repository root:

```bash
cd compatibility_service
go build -o bin/client ./cmd/client
go build -o bin/orchestrator ./cmd/orchestrator
cd ../sample_external_chaincode
go build -o bin/sample-chaincode ./cmd/server
go build -o bin/asset-transfer-chaincode ./cmd/asset-transfer-server
cd ../sample_external_resolver
go build -o bin/sample-resolver ./cmd/server
cd ..
go build -o bin/block-dump ./cmd/block-dump
```

## 3. Start Services

The sample orchestrators require `proposal-auth` configuration. Both trust the
public MSP directories for org0 and org1 and grant their members access to the
demo chaincodes/namespaces. Each remote endorser verifies the original client
independently. See [proposal authentication](docs/architecture.md#proposal-authentication)
for narrower permissions and the distinction from mTLS.

Lifecycle permissions use the same MSP trust with a separate `lifecycle-auth`
configuration. The supplied admin identities authorize installation and
organizational approval; commitment and queries use their configured policies.
The sample configurations already include these settings. See
[lifecycle permissions](docs/lifecycle.md#authentication-and-permissions).

Use five service terminals and one client terminal. Run each service block from
the repository root.

### Terminal 1: org0 CCAAS

```bash
cd sample_external_chaincode
./bin/sample-chaincode -ccid sample -address 127.0.0.1:9999
```

### Terminal 2: org1 CCAAS

```bash
cd sample_external_chaincode
./bin/sample-chaincode -ccid sample -address 127.0.0.1:10000
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
10001  org0 asset-transfer CCAAS (optional)
10002  org1 asset-transfer CCAAS (optional)
9300   sample resolver
9102   org0 orchestrator
9103   optional second org0 orchestrator
9202   org1 orchestrator
4001   Notification Service / block query
7001   Query Service
6022   Arma router
```

## 4. Install, Approve, Commit

Run from `compatibility_service` in Terminal 6. Install directly from the CCAAS
metadata directory; no separate package file is needed.

```bash
export FABRIC_LOGGING_SPEC=error
cd /home/kali/Desktop/LFX/Project/compatibility_service

./bin/orchestrator lifecycle install -c sampleconfig/admin-org0.yaml --host 127.0.0.1:9102 --path ../sample_external_chaincode/cc_go/org0_sample --label org0_sample_1
./bin/orchestrator lifecycle install -c sampleconfig/admin-org1.yaml --host 127.0.0.1:9202 --path ../sample_external_chaincode/cc_go/org1_sample --label org1_sample_1

ORG0_PKG=$(./bin/orchestrator lifecycle queryinstalled -c sampleconfig/admin-org0.yaml --host 127.0.0.1:9102 | awk -F= '/^package_id=org0_sample_1:/{print $2; exit}')
ORG1_PKG=$(./bin/orchestrator lifecycle queryinstalled -c sampleconfig/admin-org1.yaml --host 127.0.0.1:9202 | awk -F= '/^package_id=org1_sample_1:/{print $2; exit}')

./bin/orchestrator lifecycle approveformyorg -c sampleconfig/admin-org0.yaml --host 127.0.0.1:9102 -n sample -v 1.0 --sequence 1 --package-id "$ORG0_PKG" --init-required
./bin/orchestrator lifecycle approveformyorg -c sampleconfig/admin-org1.yaml --host 127.0.0.1:9202 -n sample -v 1.0 --sequence 1 --package-id "$ORG1_PKG" --init-required

./bin/orchestrator lifecycle checkcommitreadiness -c sampleconfig/admin-org0.yaml --host 127.0.0.1:9102 -n sample -v 1.0 --sequence 1 --init-required

./bin/orchestrator lifecycle commit -c sampleconfig/admin-org0.yaml --host 127.0.0.1:9102 -n sample -v 1.0 --sequence 1 --init-required

./bin/orchestrator lifecycle querycommitted -c sampleconfig/admin-org0.yaml --host 127.0.0.1:9102 
./bin/orchestrator lifecycle querycommitted -c sampleconfig/admin-org1.yaml --host 127.0.0.1:9202 


./bin/client invoke -c sampleconfig/client.yaml --namespace 1 -n sample --is-init '{"Function":"initialize","Args":["asset-init-demo","value-init-demo"]}'
```

## 5. Invoke Compat-v3

Run from `compatibility_service` in Terminal 6:

```bash
mkdir -p ../runtime/compatibility_service
./bin/client invoke -c sampleconfig/client.yaml --namespace 1 -n sample '{"Function":"compat-v3","Args":["asset-multiorg-v3","value-multiorg-v3","asset-multiorg-v3-delete"]}' | tee ../runtime/compatibility_service/compatv3-result.json
```

The client prints a larger JSON response. Check these fields:

```json
{
  "status": 200,
  "submitted": true,
  "commit_status": "COMMITTED",
  "chaincode_event": {
    "event_name": "compat-v3-event"
  }
}
```

## 6. Query State

Run from `compatibility_service`:

```bash
./bin/client query -c sampleconfig/client.yaml --namespace 1 -n sample '{"Function":"get","Args":["asset-multiorg-v3"]}'
```

Expected:

```text
value-multiorg-v3
```

Run:

```bash
./bin/client query -c sampleconfig/client.yaml --namespace 1 -n sample '{"Function":"get","Args":["asset-multiorg-v3-delete"]}'
```

Expected: empty output.

To use the official Fabric Gateway SDK through the same client binary and
configuration, run:

```bash
./bin/client evaluate -c sampleconfig/client.yaml --namespace 1 -n sample '{"Function":"get","Args":["asset-multiorg-v3"]}'
./bin/client evaluate -c sampleconfig/client.yaml --namespace 1 -n sample --target-orgs org-1 '{"Function":"get","Args":["asset-multiorg-v3"]}'
```

The command uses Gateway contract name `sample:1` and returns the original
chaincode payload. In an application using the Go Gateway SDK:

```go
network := gateway.GetNetwork("channelqc4")
contract := network.GetContract("sample:1")
result, err := contract.EvaluateTransaction("get", "asset-multiorg-v3")
```

Evaluate executes locally by default. `--target-orgs` selects an organization
through the existing resolver. Evaluation never submits a transaction; any
simulated writes or events are discarded.

Gateway `Endorse` executes the same endorsement stage as custom Invoke and
returns a prepared transaction for the SDK to sign. For example, with the
Gateway SDK's `client` package imported:

```go
proposal, err := contract.NewProposal("put", client.WithArguments("asset-gateway-demo", "prepared-value"))
if err != nil {
    return err
}
transaction, err := proposal.Endorse()
if err != nil {
    return err
}
result := transaction.Result()
prepared, err := transaction.Bytes()
if err != nil {
    return err
}
commit, err := transaction.Submit()
if err != nil {
    return err
}
txID := commit.TransactionID()
status, err := commit.Status()
if err != nil {
    return err
}
// Inspect status.Successful, status.Code, and status.BlockNumber.
```

Endorsement does not submit the simulated writes. `Submit` verifies the
client-signed envelope and native endorsements, sends the transaction, and
returns after ordering acknowledges it. It does not wait for commitment.
`CommitStatus` returns a recorded validation result immediately, or waits for
finality within the request deadline. The combined SDK
`contract.SubmitTransaction("put", "asset-gateway-demo", "committed-value")`
now runs endorsement, submission, and commit-status checking. A finality
timeout reports an unknown outcome; it does not reject or resubmit the
transaction. `ChaincodeEvents` delivers committed events through a separate
subscription. The existing custom `client invoke` also waits for finality. See
[Gateway evaluation](docs/architecture.md#gateway-evaluate) and
[Gateway endorsement](docs/architecture.md#gateway-endorse), followed by
[Gateway submission](docs/architecture.md#gateway-submit) and
[Gateway commit status](docs/architecture.md#gateway-commitstatus).

An independent application can listen for events without submitting transactions:

```go
events, err := network.ChaincodeEvents(ctx, "sample:1")
if err != nil {
    return err
}
for event := range events {
    // Process event.EventName, event.Payload, and event.TransactionID.
}
```

The default starts at the next committed block. Use `client.WithStartBlock(n)`
for replay or `client.WithCheckpoint(checkpointer)` to resume. Each subscription
ends with its context; it does not create an idle transaction tracker. Event
filtering uses signed logical chaincode identity metadata written by updated
Orchestrators. Earlier records without that identity are skipped because a
namespace alone cannot identify the emitting chaincode. See
[Gateway events](docs/architecture.md#gateway-chaincodeevents).

## 7. Inspect The Block

Run from the repository root:

```bash
txid=$(python3 -c 'import json; print(json.load(open("runtime/compatibility_service/compatv3-result.json"))["tx_id"])')
./bin/block-dump -artifacts ./artifacts -txid "$txid"
```

Expected shape:

```text
Envelope type=MESSAGE/0 channel=channelqc4
data: applicationpb.Tx namespaces=1 endorsements=1
endorsements[0] signers=2
signer[0] msp_id=org-0
signer[1] msp_id=org-1
event ... name=compat-v3-event
read_write key="asset-multiorg-v3" value="value-multiorg-v3"
read_write key="asset-multiorg-v3-delete" value=<nil>
```

## Optional: Upgrade The Compat-v3 Definition

Keep the same chaincode name and application namespace. After the sequence-1
workflow above, approve and commit sequence `2` with version `2.0`. This example
reuses the installed runtimes and does not require initialization again:

```bash
./bin/orchestrator lifecycle approveformyorg -c sampleconfig/admin-org0.yaml --host 127.0.0.1:9102 -n sample -v 2.0 --sequence 2 --package-id "$ORG0_PKG"
./bin/orchestrator lifecycle approveformyorg -c sampleconfig/admin-org1.yaml --host 127.0.0.1:9202 -n sample -v 2.0 --sequence 2 --package-id "$ORG1_PKG"
./bin/orchestrator lifecycle checkcommitreadiness -c sampleconfig/admin-org0.yaml --host 127.0.0.1:9102 -n sample -v 2.0 --sequence 2
./bin/orchestrator lifecycle commit -c sampleconfig/admin-org0.yaml --host 127.0.0.1:9102 -n sample -v 2.0 --sequence 2
./bin/orchestrator lifecycle querycommitted -c sampleconfig/admin-org0.yaml --host 127.0.0.1:9102 -n sample
./bin/client query -c sampleconfig/client.yaml --namespace 1 -n sample '{"Function":"get","Args":["asset-multiorg-v3"]}'
./bin/client invoke -c sampleconfig/client.yaml --namespace 1 -n sample '{"Function":"compat-v3","Args":["asset-multiorg-v3","value-after-upgrade","asset-multiorg-v3-delete"]}' | tee ../runtime/compatibility_service/compatv3-result.json
```

The committed definition reports version `2.0`, sequence `2`. The query still
returns `value-multiorg-v3`; the invocation reports that previous committed value
and then updates it. Inspect the new block using step 7. Client calls do not need
a version flag. The next definition update uses sequence `3`, regardless of its
version label. See [lifecycle metadata migration](docs/lifecycle.md#commit) when
upgrading a ledger created with the earlier `name:version` prototype.

## Optional: Fabric Samples Asset Transfer

This runs the actual Go [Asset Transfer Basic contract](sample_external_chaincode/asset_transfer/README.md)
from Fabric Samples, with unchanged business logic. A small launcher serves
the Fabric Contract API chaincode through CCAAS.

Use the network and service setup in steps 1–3 above. The sample orchestrator
configs already route the committed `asset-transfer` definition to the two endpoints below; restart
the orchestrators if they were started before this configuration was added.
The existing resolver and compatibility sample can continue running.

Start two additional service terminals, each from the repository root:

```bash
# org0 asset-transfer CCAAS
cd sample_external_chaincode
./bin/asset-transfer-chaincode -ccid asset-transfer -address 127.0.0.1:10001
```

```bash
# org1 asset-transfer CCAAS
cd sample_external_chaincode
./bin/asset-transfer-chaincode -ccid asset-transfer -address 127.0.0.1:10002
```

Run the normal lifecycle commands from `compatibility_service` in the client
terminal. Package IDs are taken from each install result, so existing sample
packages do not affect which package is approved:

```bash
export FABRIC_LOGGING_SPEC=error
mkdir -p ../runtime/compatibility_service/asset-transfer

./bin/orchestrator lifecycle install -c sampleconfig/admin-org0.yaml --host 127.0.0.1:9102 --path ../sample_external_chaincode/cc_go/org0_asset_transfer --label org0_asset_transfer_1 | tee ../runtime/compatibility_service/asset-transfer/install-org0.txt
./bin/orchestrator lifecycle install -c sampleconfig/admin-org1.yaml --host 127.0.0.1:9202 --path ../sample_external_chaincode/cc_go/org1_asset_transfer --label org1_asset_transfer_1 | tee ../runtime/compatibility_service/asset-transfer/install-org1.txt

ORG0_ASSET_PKG=$(awk -F= '/^package_id=/{print $2; exit}' ../runtime/compatibility_service/asset-transfer/install-org0.txt)
ORG1_ASSET_PKG=$(awk -F= '/^package_id=/{print $2; exit}' ../runtime/compatibility_service/asset-transfer/install-org1.txt)

./bin/orchestrator lifecycle approveformyorg -c sampleconfig/admin-org0.yaml --host 127.0.0.1:9102 -n asset-transfer -v 1.0 --sequence 1 --package-id "$ORG0_ASSET_PKG"
./bin/orchestrator lifecycle approveformyorg -c sampleconfig/admin-org1.yaml --host 127.0.0.1:9202 -n asset-transfer -v 1.0 --sequence 1 --package-id "$ORG1_ASSET_PKG"

./bin/orchestrator lifecycle checkcommitreadiness -c sampleconfig/admin-org0.yaml --host 127.0.0.1:9102 -n asset-transfer -v 1.0 --sequence 1
./bin/orchestrator lifecycle commit -c sampleconfig/admin-org0.yaml --host 127.0.0.1:9102 -n asset-transfer -v 1.0 --sequence 1

./bin/orchestrator lifecycle querycommitted -c sampleconfig/admin-org0.yaml --host 127.0.0.1:9102 -n asset-transfer -v 1.0
./bin/orchestrator lifecycle querycommitted -c sampleconfig/admin-org1.yaml --host 127.0.0.1:9202 -n asset-transfer -v 1.0
```

Expect `ledger_status=COMMITTED` from lifecycle commitment and a committed
definition on both organizations. This records the definition in lifecycle
namespace `0`; application state goes into the existing two-organization
namespace `1` when invoked below.

Create an asset owned by Alice, query it, then transfer it to Bob:

```bash
ASSET_ID="asset-transfer-$(date +%s)"
printf '%s\n' "$ASSET_ID" > ../runtime/compatibility_service/asset-transfer/asset-id.txt

./bin/client invoke -c sampleconfig/client.yaml --namespace 1 -n asset-transfer "{\"Function\":\"CreateAsset\",\"Args\":[\"$ASSET_ID\",\"blue\",\"5\",\"Alice\",\"300\"]}" | tee ../runtime/compatibility_service/asset-transfer/create-result.json

./bin/client query -c sampleconfig/client.yaml --namespace 1 -n asset-transfer "{\"Function\":\"ReadAsset\",\"Args\":[\"$ASSET_ID\"]}"

./bin/client invoke -c sampleconfig/client.yaml --namespace 1 -n asset-transfer "{\"Function\":\"TransferAsset\",\"Args\":[\"$ASSET_ID\",\"Bob\"]}" | tee ../runtime/compatibility_service/asset-transfer/transfer-result.json

./bin/client query -c sampleconfig/client-org1.yaml --namespace 1 -n asset-transfer "{\"Function\":\"ReadAsset\",\"Args\":[\"$ASSET_ID\"]}" | tee ../runtime/compatibility_service/asset-transfer/read-after-transfer.json
```

Both invoke results must show `submitted: true` and
`commit_status: "COMMITTED"`. `TransferAsset` returns the previous owner,
`Alice`; the final query through org1 must show `"Owner":"Bob"`.

Inspect and save the actual transfer block from the repository root:

```bash
cd ..
txid=$(python3 -c 'import json; print(json.load(open("runtime/compatibility_service/asset-transfer/transfer-result.json"))["tx_id"])')
./bin/block-dump -artifacts ./artifacts -txid "$txid" | tee runtime/compatibility_service/asset-transfer/transfer-block.txt
./bin/block-dump -artifacts ./artifacts -txid "$txid" -raw-json | sed '1d' > runtime/compatibility_service/asset-transfer/transfer-block.json
```

The dump shows two endorsers (`org-0` and `org-1`), a read-write entry for your
asset containing `"Owner":"Bob"`, and metadata containing `TransferAsset`
and the returned previous owner. The upstream contract does not emit events.
`GetAllAssets` remains unsupported because it requires a range query; use
`ReadAsset` to query an exact key.

For another run, use a new `ASSET_ID`. Committed definitions are restored from
the ledger after an orchestrator restart; installed packages and approvals are
not restored. The asset-transfer static resolver entries still locate its
runtimes, so this demo can continue invoking the committed definition without
another install, approval, or commitment. Reinstalling and approving the exact
committed definition can restore its org-local package binding. Committing an
upgrade requires the current sequence plus one, even when the version changes.

## Optional: Init-Required Lifecycle

Use `--init-required` to block normal invoke/query until one init transaction
commits and its initialized marker is confirmed. `--is-init` calls the actual
chaincode `Init()` entry point. The sample's `Init()` accepts `initialize` with a
key and value to store. The current Fabric-X Committer requires at least one
write in each transaction namespace, so a no-op `Init()` cannot commit.

```bash
ORG0_PKG=$(./bin/orchestrator lifecycle queryinstalled -c sampleconfig/admin-org0.yaml --host 127.0.0.1:9102 | awk -F= '/^package_id=org0_sample_1:/{print $2; exit}')
ORG1_PKG=$(./bin/orchestrator lifecycle queryinstalled -c sampleconfig/admin-org1.yaml --host 127.0.0.1:9202 | awk -F= '/^package_id=org1_sample_1:/{print $2; exit}')
./bin/orchestrator lifecycle approveformyorg -c sampleconfig/admin-org0.yaml --host 127.0.0.1:9102 -n sample-init -v 1.0 --sequence 1 --package-id "$ORG0_PKG" --init-required
./bin/orchestrator lifecycle approveformyorg -c sampleconfig/admin-org1.yaml --host 127.0.0.1:9202 -n sample-init -v 1.0 --sequence 1 --package-id "$ORG1_PKG" --init-required
./bin/orchestrator lifecycle checkcommitreadiness -c sampleconfig/admin-org0.yaml --host 127.0.0.1:9102 -n sample-init -v 1.0 --sequence 1 --init-required
./bin/orchestrator lifecycle commit -c sampleconfig/admin-org0.yaml --host 127.0.0.1:9102 -n sample-init -v 1.0 --sequence 1 --init-required
./bin/client invoke -c sampleconfig/client.yaml --namespace 1 -n sample-init --is-init '{"Function":"initialize","Args":["asset-init-demo","value-init-demo"]}'
./bin/client invoke -c sampleconfig/client.yaml --namespace 1 -n sample-init '{"Function":"compat-v3","Args":["asset-after-init","value-after-init","asset-after-init-delete"]}'
```

## Optional: Multi-Orchestrator Candidate Routing

Start a second org0 orchestrator:

```bash
cd compatibility_service
./bin/orchestrator -c sampleconfig/orchestrator0B.yaml --log-level info:grpc=error
```

Restart the sample resolver with org0 candidates in priority order:

```bash
cd sample_external_resolver
pkill -f 'sample-resolver.*127.0.0.1:9300' || true
./bin/sample-resolver -listen 127.0.0.1:9300 -tls-mode mtls -tls-cert ../artifacts/peerOrganizations/peer-org-0/peers/helper.peer-org-0/tls/server.crt -tls-key ../artifacts/peerOrganizations/peer-org-0/peers/helper.peer-org-0/tls/server.key -client-ca ../artifacts/peerOrganizations/peer-org-0/tlsca/tlsca.peer-org-0-cert.pem,../artifacts/peerOrganizations/peer-org-1/tlsca/tlsca.peer-org-1-cert.pem -org0-orchestrator 127.0.0.1:9103,127.0.0.1:9102
```

Invoke through org1 so the org1 orchestrator asks the resolver for org0
candidates:

```bash
cd compatibility_service
./bin/client invoke -c sampleconfig/client-org1.yaml --namespace 1 -n sample '{"Function":"compat-v3","Args":["asset-org1-route","value-org1-route","asset-org1-route-delete"]}'
```

The org1 orchestrator log should include:

```text
remote orchestrator resolver dynamic grpc match requester_msp=org-1 target_msp=org-0 candidates=2
requesting remote orchestrator msp=org-0 endpoint=127.0.0.1:9103
```

## Service Logs

Terminals 4 and 5 show orchestrator, helper, shim, and gRPC logs. Terminal 6
prints the client JSON response.

Useful lines:

```text
[orchestrator] ProcessProposal -> tx=... namespace=1 chaincode=sample version=1.0 fn=compat-v3
[shim] Execute -> tx=... ccaas connect endpoint=127.0.0.1:9999
[helper] ProcessProposal -> tx=... chaincode execution completed status=200
[orchestrator] submitAndWaitFinality -> tx=... finality status=COMMITTED block=...
```

## Tests

Run unit tests:

```bash
cd compatibility_service
go test ./pkg/helper ./pkg/lifecycle ./pkg/orchestrator ./pkg/shim
```

Run real-network integration tests after setup:

```bash
cd compatibility_service
go test -tags=e2e ./integration/e2e -count=1 -v
```

To run the sample `compat-v3` lifecycle upgrade simulation on its own:

```bash
go test -tags=e2e ./integration/e2e -run '^TestLifecycleUpgradeUsesNameAndSequenceWithCompatV3$' -count=1 -v
```

It commits versions `1.0`, `2.0`, and `2.1` at sequences `1`, `2`, and `3`, invokes
without a client version flag, verifies preserved state and both endorsements,
and checks startup hydration. Results and the actual block are saved under
`runtime/compatibility_service/lifecycle-upgrade/`.

To verify the Gateway SDK, its CLI command, binary responses, remote targeting,
authorization, and evaluation without ledger updates:

```bash
go test -tags=e2e ./integration/e2e -run '^TestGatewayEvaluateSDK$' -count=1 -v
```

The test commits its setup data, then verifies that all evaluations leave the
ledger height unchanged. It also restarts both Orchestrators and both CCaaS
services on the same ports, reuses the SDK connection, and checks definition
reload and static endpoint resolution without reinstalling packages. Its report is saved under
`runtime/compatibility_service/gateway-evaluate/`.

To verify Gateway endorsement, native signatures, SDK transaction parsing and
client signing, restart recovery, and continued custom Invoke commitment:

```bash
go test -tags=e2e ./integration/e2e -run '^TestGatewayEndorseSDK$' -count=1 -v
```

This test checks that endorsement leaves the ledger height unchanged, restarts
both Orchestrators, and then commits through custom Invoke. Its report and a
signed prepared transaction are saved under
`runtime/compatibility_service/gateway-endorse/`.

To verify SDK submission, client authorization, exact native transaction bytes,
and duplicate handling:

```bash
go test -tags=e2e ./integration/e2e -run '^TestGatewaySubmitSDK$' -count=1 -v
```

This test endorses through org0, restarts org1, stops both CCaaS services, and
submits the prepared transaction through org1. It checks actual commitment with
the Fabric-X Query Service, compares the signed native bytes with the block,
and verifies that retries submit only once. Reports and protobuf evidence are
saved under `runtime/compatibility_service/gateway-submit/`.

To verify SDK commit status before submission, after finality, and through a
different restarted Orchestrator, including rejected transactions, missed
notifications, request timeouts, and the complete SDK submit-and-wait flow:

```bash
go test -tags=e2e ./integration/e2e -run '^TestGatewayCommitStatus(SDK|MissedNotification)$' -count=1 -v
```

These tests use the existing network and isolate notification faults in a test
proxy. Results and a real committed block are saved under
`runtime/compatibility_service/gateway-commit-status/`.

To verify live SDK events, filtering between chaincodes sharing a namespace,
custom Invoke event delivery, and replay/checkpoints through a restarted
Orchestrator with CCaaS stopped:

```bash
go test -tags=e2e ./integration/e2e -run '^TestGatewayChaincodeEventsSDK$' -count=1 -v
```

This uses the existing `compat-v3` chaincode function and the normal folder
installation lifecycle. Reports and both committed blocks are saved under
`runtime/compatibility_service/gateway-events/`.

The integration tests start their own orchestrators and CCAAS processes against
the running Fabric-X network. To verify original-client authentication and
produce one committed transaction:

```bash
cd compatibility_service
go test -tags=e2e ./integration/e2e -run '^TestClientProposalAuthentication$' -count=1 -v
cd ..
txid=$(python3 -c 'import json; print(json.load(open("runtime/compatibility_service/e2e/proposal-auth-result.json"))["tx_id"])')
./bin/block-dump -artifacts ./artifacts -txid "$txid"
```

The authentication test checks rejected proposals against both organizations
over mTLS, confirms they do not create blocks, and then commits an authorized
transaction with both endorsements and a chaincode event.

The policy integration test creates an isolated namespace, updates its policy,
and verifies a committed transaction with two distinct org0 signing certificates
after trying a duplicate-certificate instance. It also checks that existing
Orchestrators sign using the updated namespace policy version:

```bash
cd compatibility_service
go test -tags=e2e ./integration/e2e -run '^TestDistinctSameOrgEndorsementsAndPolicyUpdate$' -count=1 -v
```

Its result and block are saved under
`runtime/compatibility_service/endorsement-policy/`.

Avoid `go test ./...` from the repository root after the network has started;
Docker-owned files under `storage/` can interfere with package discovery.

## Stop And Clean Up

Run from the repository root:

```bash
./scripts/stop-network.sh
pkill -f 'sample-chaincode'
pkill -f 'asset-transfer-chaincode'
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

## Development Notes

- Lifecycle installed packages are kept in memory inside each orchestrator.
- Committed lifecycle definitions are written to Fabric-X ledger namespace `0`.
- CCAAS and remote orchestrator endpoints are resolved lazily by static config
  or by the sample resolver.
- The sample resolver accepts comma-separated `-org0-orchestrator` and
  `-org1-orchestrator` values and returns them as ordered candidates.
- The resolver is user-owned. Production routing, health checks, region
  selection, and load preference belong behind that API.
- Lifecycle identity is channel plus chaincode name. Version is descriptive;
  sequence is the revision counter across versions. Invocations use the active
  definition by name and keep the requested Fabric-X application namespace.

Additional developer notes:

```text
docs/architecture.md       execution path and helper semantics
docs/lifecycle.md          compatibility lifecycle model
docs/resolver.md           resolver API and multi-orchestrator routing
```
