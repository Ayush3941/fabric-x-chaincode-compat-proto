# Sample External Chaincode

This module provides the Go chaincode-as-a-service process used by the
compatibility demo.

The chaincode is ordinary Fabric chaincode served through
`shim.ChaincodeServer`. The compatibility service connects to it through the
Fabric CCAAS shim protocol.

## Build

```bash
go build -o bin/sample-chaincode ./cmd/server
```

## Run

Org0:

```bash
./bin/sample-chaincode -ccid '0:sample' -address 127.0.0.1:9999
```

Org1:

```bash
./bin/sample-chaincode -ccid '0:sample' -address 127.0.0.1:10000
```

TLS is disabled for the sample CCAAS endpoints. The resolver returns `tls_mode:
none` for chaincode service resolution.

## Functions

```text
get key
put key value
putget key value
del key
compatv1 key value delete_key
compatv2 key value delete_key
compat-v3 key value delete_key
```

`compat-v3` is the current broad compatibility check. It exercises state reads
and writes, delete, composite keys, creator/MSP extraction, signed proposal,
binding, timestamp, transient data, decorations, string args, and named events.

## Package Inputs

CCAAS package inputs live under `cc_go/`:

```text
cc_go/org0_sample/   connection metadata for org0 at 127.0.0.1:9999
cc_go/org1_sample/   connection metadata for org1 at 127.0.0.1:10000
cc_go/e2e/           connection metadata for e2e test chaincode
```

The lifecycle package command builds a Fabric-style external chaincode package
from one of these directories:

```bash
../compatibility_service/bin/orchestrator lifecycle package --path ./cc_go/org0_sample --label org0_sample_1 --output ./cc_package/org0_sample/org0_sample.tgz
```
