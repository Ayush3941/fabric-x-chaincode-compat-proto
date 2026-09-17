# Sample External Chaincode

This is a minimal Go chaincode-as-a-service process for the compatibility service smoke test.

It listens on `127.0.0.1:9999` with TLS disabled, matching:

```yaml
chaincode-service:
  endpoint:
    host: 127.0.0.1
    port: 9999
  tls:
    mode: none
```

Supported functions:

- `get key`
- `put key value`
- `putget key value`
- `del key`
- `compatv1 key value delete_key`
- `compatv2 key value delete_key`

Build and run:

```bash
go build -o bin/sample-chaincode ./cmd/server
./bin/sample-chaincode -address 127.0.0.1:9999 -ccid 0:sample
```

CCAAS package inputs:

```text
cc_go/org0_sample/   package metadata for cmd/server at 127.0.0.1:9999
cc_go/org1_sample/   package metadata for cmd/server at 127.0.0.1:10000
cc_go/e2e/           package metadata for cmd/e2e-server
```
