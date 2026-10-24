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
- Lifecycle package, install, approve, readiness, commit, query, and init gate
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

Use five service terminals and one client terminal. Run each service block from
the repository root.

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

## 4. Package, Install, Approve, Commit

Run from `compatibility_service` in Terminal 6:

```bash
export FABRIC_LOGGING_SPEC=error
cd /home/kali/Desktop/LFX/Project/compatibility_service

./bin/orchestrator lifecycle package --path ../sample_external_chaincode/cc_go/org0_sample --label org0_sample_1 --output ../sample_external_chaincode/cc_package/org0_sample/org0_sample.tgz
./bin/orchestrator lifecycle package --path ../sample_external_chaincode/cc_go/org1_sample --label org1_sample_1 --output ../sample_external_chaincode/cc_package/org1_sample/org1_sample.tgz

./bin/orchestrator lifecycle install -c sampleconfig/admin-org0.yaml --host 127.0.0.1:9102 ../sample_external_chaincode/cc_package/org0_sample/org0_sample.tgz
./bin/orchestrator lifecycle install -c sampleconfig/admin-org1.yaml --host 127.0.0.1:9202 ../sample_external_chaincode/cc_package/org1_sample/org1_sample.tgz

ORG0_PKG=$(./bin/orchestrator lifecycle queryinstalled -c sampleconfig/admin-org0.yaml --host 127.0.0.1:9102 | awk -F= '/^package_id=org0_sample_1:/{print $2; exit}')
ORG1_PKG=$(./bin/orchestrator lifecycle queryinstalled -c sampleconfig/admin-org1.yaml --host 127.0.0.1:9202 | awk -F= '/^package_id=org1_sample_1:/{print $2; exit}')

./bin/orchestrator lifecycle approveformyorg -c sampleconfig/admin-org0.yaml --host 127.0.0.1:9102 -n sample -v 1.0 --sequence 1 --package-id "$ORG0_PKG"
./bin/orchestrator lifecycle approveformyorg -c sampleconfig/admin-org1.yaml --host 127.0.0.1:9202 -n sample -v 1.0 --sequence 1 --package-id "$ORG1_PKG"

./bin/orchestrator lifecycle checkcommitreadiness -c sampleconfig/admin-org0.yaml --host 127.0.0.1:9102 -n sample -v 1.0 --sequence 1
./bin/orchestrator lifecycle commit -c sampleconfig/admin-org0.yaml --host 127.0.0.1:9102 -n sample -v 1.0 --sequence 1

./bin/orchestrator lifecycle querycommitted -c sampleconfig/admin-org0.yaml --host 127.0.0.1:9102 -n sample -v 1.0
./bin/orchestrator lifecycle querycommitted -c sampleconfig/admin-org1.yaml --host 127.0.0.1:9202 -n sample -v 1.0
```

Expected commit output includes:

```text
ledger_status=COMMITTED
org-0=true
org-1=true
```

## 5. Invoke Compat-v3

Run from `compatibility_service` in Terminal 6:

```bash
mkdir -p ../runtime/compatibility_service
./bin/client invoke -c sampleconfig/client.yaml --namespace 1 -n sample -v 1.0 '{"Function":"compat-v3","Args":["asset-multiorg-v3","value-multiorg-v3","asset-multiorg-v3-delete"]}' | tee ../runtime/compatibility_service/compatv3-result.json
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
./bin/client query -c sampleconfig/client.yaml --namespace 1 -n sample -v 1.0 '{"Function":"get","Args":["asset-multiorg-v3"]}'
```

Expected:

```text
value-multiorg-v3
```

Run:

```bash
./bin/client query -c sampleconfig/client.yaml --namespace 1 -n sample -v 1.0 '{"Function":"get","Args":["asset-multiorg-v3-delete"]}'
```

Expected: empty output.

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

## Optional: Fabric Samples Asset Transfer

This runs the actual Go [Asset Transfer Basic contract](sample_external_chaincode/asset_transfer/README.md)
from Fabric Samples, with unchanged business logic. A small launcher serves
the Fabric Contract API chaincode through CCAAS.

Use the network and service setup in steps 1–3 above. The sample orchestrator
configs already route `asset-transfer:1.0` to the two endpoints below; restart
the orchestrators if they were started before this configuration was added.
The existing resolver and compatibility sample can continue running.

Start two additional service terminals, each from the repository root:

```bash
# org0 asset-transfer CCAAS
cd sample_external_chaincode
./bin/asset-transfer-chaincode -ccid 'asset-transfer:1.0' -address 127.0.0.1:10001
```

```bash
# org1 asset-transfer CCAAS
cd sample_external_chaincode
./bin/asset-transfer-chaincode -ccid 'asset-transfer:1.0' -address 127.0.0.1:10002
```

Run the normal lifecycle commands from `compatibility_service` in the client
terminal. Package IDs are taken from each install result, so existing sample
packages do not affect which package is approved:

```bash
export FABRIC_LOGGING_SPEC=error
mkdir -p ../runtime/compatibility_service/asset-transfer

./bin/orchestrator lifecycle package --path ../sample_external_chaincode/cc_go/org0_asset_transfer --label org0_asset_transfer_1 --output ../sample_external_chaincode/cc_package/org0_asset_transfer/org0_asset_transfer.tgz
./bin/orchestrator lifecycle package --path ../sample_external_chaincode/cc_go/org1_asset_transfer --label org1_asset_transfer_1 --output ../sample_external_chaincode/cc_package/org1_asset_transfer/org1_asset_transfer.tgz

./bin/orchestrator lifecycle install -c sampleconfig/admin-org0.yaml --host 127.0.0.1:9102 ../sample_external_chaincode/cc_package/org0_asset_transfer/org0_asset_transfer.tgz | tee ../runtime/compatibility_service/asset-transfer/install-org0.txt
./bin/orchestrator lifecycle install -c sampleconfig/admin-org1.yaml --host 127.0.0.1:9202 ../sample_external_chaincode/cc_package/org1_asset_transfer/org1_asset_transfer.tgz | tee ../runtime/compatibility_service/asset-transfer/install-org1.txt

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

./bin/client invoke -c sampleconfig/client.yaml --namespace 1 -n asset-transfer -v 1.0 "{\"Function\":\"CreateAsset\",\"Args\":[\"$ASSET_ID\",\"blue\",\"5\",\"Alice\",\"300\"]}" | tee ../runtime/compatibility_service/asset-transfer/create-result.json

./bin/client query -c sampleconfig/client.yaml --namespace 1 -n asset-transfer -v 1.0 "{\"Function\":\"ReadAsset\",\"Args\":[\"$ASSET_ID\"]}"

./bin/client invoke -c sampleconfig/client.yaml --namespace 1 -n asset-transfer -v 1.0 "{\"Function\":\"TransferAsset\",\"Args\":[\"$ASSET_ID\",\"Bob\"]}" | tee ../runtime/compatibility_service/asset-transfer/transfer-result.json

./bin/client query -c sampleconfig/client-org1.yaml --namespace 1 -n asset-transfer -v 1.0 "{\"Function\":\"ReadAsset\",\"Args\":[\"$ASSET_ID\"]}" | tee ../runtime/compatibility_service/asset-transfer/read-after-transfer.json
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
another install, approval, or commitment. The current lifecycle implementation
requires the next sequence for a new approval after restart; do not repeat
sequence-1 approvals against a definition that is already committed.

## Optional: Init-Required Lifecycle

Use `--init-required` to block normal invoke/query until one init transaction
commits.

```bash
ORG0_PKG=$(./bin/orchestrator lifecycle queryinstalled -c sampleconfig/admin-org0.yaml --host 127.0.0.1:9102 | awk -F= '/^package_id=org0_sample_1:/{print $2; exit}')
ORG1_PKG=$(./bin/orchestrator lifecycle queryinstalled -c sampleconfig/admin-org1.yaml --host 127.0.0.1:9202 | awk -F= '/^package_id=org1_sample_1:/{print $2; exit}')
./bin/orchestrator lifecycle approveformyorg -c sampleconfig/admin-org0.yaml --host 127.0.0.1:9102 -n sample-init -v 1.0 --sequence 1 --package-id "$ORG0_PKG" --init-required
./bin/orchestrator lifecycle approveformyorg -c sampleconfig/admin-org1.yaml --host 127.0.0.1:9202 -n sample-init -v 1.0 --sequence 1 --package-id "$ORG1_PKG" --init-required
./bin/orchestrator lifecycle checkcommitreadiness -c sampleconfig/admin-org0.yaml --host 127.0.0.1:9102 -n sample-init -v 1.0 --sequence 1 --init-required
./bin/orchestrator lifecycle commit -c sampleconfig/admin-org0.yaml --host 127.0.0.1:9102 -n sample-init -v 1.0 --sequence 1 --init-required
./bin/client invoke -c sampleconfig/client.yaml --namespace 1 -n sample-init -v 1.0 --is-init '{"Function":"compat-v3","Args":["asset-init-demo","value-init-demo","asset-init-demo-delete"]}'
./bin/client invoke -c sampleconfig/client.yaml --namespace 1 -n sample-init -v 1.0 '{"Function":"compat-v3","Args":["asset-after-init","value-after-init","asset-after-init-delete"]}'
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
./bin/client invoke -c sampleconfig/client-org1.yaml --namespace 1 -n sample -v 1.0 '{"Function":"compat-v3","Args":["asset-org1-route","value-org1-route","asset-org1-route-delete"]}'
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
[orchestrator] ProcessProposal -> tx=... namespace=1 chaincode=sample:1.0 fn=compat-v3
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
- Current lifecycle identity is `name:version`; `sequence` is the upgrade
  counter for that chaincode identity.

Additional developer notes:

```text
docs/architecture.md       execution path and helper semantics
docs/lifecycle.md          compatibility lifecycle model
docs/resolver.md           resolver API and multi-orchestrator routing
```
