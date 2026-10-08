# Resolver API

The resolver API is the compatibility layer's boundary for user-owned discovery.
The compatibility layer asks for endpoints; the resolver decides where they
come from and in what priority order.

The sample implementation lives in `sample_external_resolver/`.

## Operations

```text
CC_resolution
REMOTE_ORCHESTRATOR_resolution
```

Both operations use the JSON-over-gRPC service in
`compatibility_service/pkg/ccresolver`.

## Chaincode Resolution

Request:

```json
{
  "operation": "CC_resolution",
  "msp_id": "org-0",
  "name": "sample",
  "version": "1.0",
  "sequence": 1
}
```

Response:

```json
{
  "found": true,
  "address": "127.0.0.1:9999",
  "tls_mode": "none",
  "package_id": "org0_sample_1:<hash>",
  "sequence": 1
}
```

The current shim connector supports `tls_mode: none` for CCAAS endpoints.

## Remote Orchestrator Resolution

Request:

```json
{
  "operation": "REMOTE_ORCHESTRATOR_resolution",
  "requester_msp_id": "org-1",
  "target_msp_id": "org-0",
  "channel_id": "channelqc4",
  "namespace": "1",
  "name": "sample",
  "version": "1.0",
  "sequence": 1
}
```

Response:

```json
{
  "found": true,
  "candidates": [
    {
      "msp_id": "org-0",
      "instance_id": "org0B",
      "address": "127.0.0.1:9103",
      "tls_mode": "mtls"
    },
    {
      "msp_id": "org-0",
      "instance_id": "org0A",
      "address": "127.0.0.1:9102",
      "tls_mode": "mtls"
    }
  ]
}
```

The compatibility layer tries candidates in order. If the first endpoint fails,
it tries the next endpoint for the same MSP.

## Routing Policy

Routing policy belongs outside the compatibility layer. A resolver can choose
candidates by:

```text
health status
region or locality
requester MSP
target MSP
namespace
chaincode name/version/sequence
load or maintenance state
static registry
Kubernetes service discovery
HTTP, gRPC, DNS, or database lookup
```

The compatibility layer only enforces the protocol contract and endorsement
matching rules.
