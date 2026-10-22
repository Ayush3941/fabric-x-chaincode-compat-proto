# Fabric Samples Asset Transfer Basic

`chaincode/smartcontract.go` is copied without changes from Hyperledger
[fabric-samples](https://github.com/hyperledger/fabric-samples/blob/5789681b4f4d24e58fa40f19a69f5496892374b6/asset-transfer-basic/chaincode-go/chaincode/smartcontract.go),
revision `5789681b4f4d24e58fa40f19a69f5496892374b6`. Its SHA-256 is
`fe769ed788384836252e03e6d447b6f1a27f529d69f6989ffa778f64605fe6b0`.
The upstream Apache-2.0 license is included in [LICENSE](LICENSE).

The local [launcher](../cmd/asset-transfer-server/main.go) creates the standard
Fabric Contract API chaincode and serves it through `shim.ChaincodeServer`.
Only the startup method changes from the upstream executable; the contract's
business logic is unchanged.

Follow the complete [asset-transfer workflow](../../README.md#optional-fabric-samples-asset-transfer)
in the root README to build, start one runtime per organization, package,
install, approve, commit, create an asset, transfer ownership, and inspect the
committed Fabric-X block.

`InitLedger`, `CreateAsset`, `ReadAsset`, `UpdateAsset`, `DeleteAsset`,
`AssetExists`, and `TransferAsset` use direct key operations supported by the
compatibility layer. `GetAllAssets` is retained from upstream but cannot run
because `GetStateByRange` is unsupported. The upstream contract emits no
chaincode events. Use `query` for `ReadAsset` and `AssetExists`; use `invoke`
for operations that update state.

The demo uses Fabric-X namespace `1`, whose policy requires org0 and org1.
Both organizations execute their own CCaaS runtime and endorse the same state
change. Namespace `1` is shared with the existing compatibility sample, so use
the unique asset ID shown in the workflow to avoid overwriting its keys.
