# Sample Resolver

This module is a small user-owned resolver service used by the demo. It is not
part of the compatibility service runtime.

The compatibility layer calls this service when static config does not already
provide the needed endpoint.

## Build

```bash
go build -o bin/sample-resolver ./cmd/server
```

## Run

```bash
./bin/sample-resolver -listen 127.0.0.1:9300 -tls-mode mtls -tls-cert ../artifacts/peerOrganizations/peer-org-0/peers/helper.peer-org-0/tls/server.crt -tls-key ../artifacts/peerOrganizations/peer-org-0/peers/helper.peer-org-0/tls/server.key -client-ca ../artifacts/peerOrganizations/peer-org-0/tlsca/tlsca.peer-org-0-cert.pem,../artifacts/peerOrganizations/peer-org-1/tlsca/tlsca.peer-org-1-cert.pem
```

If port `9300` is already in use:

```bash
pkill -f 'sample-resolver.*127.0.0.1:9300'
```

## Chaincode Resolution

For `CC_resolution`, the resolver returns the org-local CCAAS endpoint and
package ID:

```json
{
  "found": true,
  "address": "127.0.0.1:9999",
  "tls_mode": "none",
  "package_id": "org0_sample_1:<hash>",
  "sequence": 1
}
```

## Remote Orchestrator Resolution

For `REMOTE_ORCHESTRATOR_resolution`, the resolver returns ordered orchestrator
candidates for one MSP:

```json
{
  "found": true,
  "candidates": [
    { "msp_id": "org-0", "instance_id": "org-0-1", "address": "127.0.0.1:9103", "tls_mode": "mtls" },
    { "msp_id": "org-0", "instance_id": "org-0-2", "address": "127.0.0.1:9102", "tls_mode": "mtls" }
  ]
}
```

Use comma-separated flags to set the candidate order:

```bash
./bin/sample-resolver -listen 127.0.0.1:9300 -tls-mode mtls -tls-cert ../artifacts/peerOrganizations/peer-org-0/peers/helper.peer-org-0/tls/server.crt -tls-key ../artifacts/peerOrganizations/peer-org-0/peers/helper.peer-org-0/tls/server.key -client-ca ../artifacts/peerOrganizations/peer-org-0/tlsca/tlsca.peer-org-0-cert.pem,../artifacts/peerOrganizations/peer-org-1/tlsca/tlsca.peer-org-1-cert.pem -org0-orchestrator 127.0.0.1:9103,127.0.0.1:9102
```

Production resolvers can replace this sample with any backend: static registry,
HTTP, DNS, Kubernetes discovery, gRPC, database lookup, health checks, locality,
or load-based priority.
