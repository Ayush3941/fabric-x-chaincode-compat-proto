// SPDX-License-Identifier: Apache-2.0

package main

import (
	"flag"
	"log"

	"sample_external_chaincode/asset_transfer/chaincode"

	"github.com/hyperledger/fabric-chaincode-go/v2/shim"
	"github.com/hyperledger/fabric-contract-api-go/v2/contractapi"
)

func main() {
	ccid := flag.String("ccid", "asset-transfer:1.0", "chaincode ID sent during REGISTER")
	address := flag.String("address", "127.0.0.1:10001", "CCAAS listen address")
	flag.Parse()

	cc, err := contractapi.NewChaincode(&chaincode.SmartContract{})
	if err != nil {
		log.Fatalf("create asset-transfer chaincode: %v", err)
	}
	server := &shim.ChaincodeServer{
		CCID:     *ccid,
		Address:  *address,
		CC:       cc,
		TLSProps: shim.TLSProperties{Disabled: true},
	}
	log.Printf("asset-transfer CCAAS listening on %s", *address)
	if err := server.Start(); err != nil {
		log.Fatalf("serve asset-transfer chaincode: %v", err)
	}
}
