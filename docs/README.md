# Fabric-X Project Environment

This folder runs a local Fabric-X test network with a real Arma orderer path,
Fabric-X committer services, block ledger, query service, and a small SDK smoke
client.

## Main Commands

```bash
./scripts/build-images.sh
./scripts/generate-artifacts.sh
./scripts/start-network.sh
./scripts/create-namespace.sh
./scripts/smoke.sh
./scripts/inspect-blocks.sh --from 0
```

`scripts/smoke.sh` submits a hard-coded Fabric-X `applicationpb.Tx` built from a
local read/write set. The default transaction is a blind write. To test an MVCC
read-write update after the first write:

```bash
go run ./cmd/rws-smoke --wait --query --key asset2 --value updated --read-version 0
```

`scripts/inspect-blocks.sh` uses the committer sidecar `BlockQueryService` on
port `4001`, so it reads the real block ledger maintained by the running
committer. The row/status smoke client still uses the Query Service on port
`7001`.
