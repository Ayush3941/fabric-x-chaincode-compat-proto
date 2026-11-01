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
  "channel_id": "channelqc4",
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

Version and sequence in this request come from the active committed definition,
not from a client-selected historical version. Static entries may set
`channel-id` to limit the mapping to one channel. Caches are scoped by channel,
organization, name, and sequence. An installed package approved for the active
sequence takes precedence over resolver lookup.

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

An endpoint returning an endorsement does not necessarily finish collection.
The Orchestrator validates the actual signing certificate and namespace
signature, then checks whether the collected distinct identities satisfy the
policy. It can continue to other candidates in the same MSP for a different
role or an additional signer. The resolver does not need to advertise roles or
certificates; endpoint labels and metadata are not treated as proof of identity.

To support policies requiring multiple signers in one organization, return
instances configured with different signing certificates. Multiple endpoints
using the same certificate count as one signer. Additional endpoints belonging
to the initiating organization can be configured using its own MSP ID; they
participate in endorsement collection and are skipped by lifecycle coordination.

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
