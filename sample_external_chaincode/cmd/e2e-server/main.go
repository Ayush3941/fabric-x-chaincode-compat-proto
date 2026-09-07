// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/hyperledger/fabric-chaincode-go/v2/shim"
	pb "github.com/hyperledger/fabric-protos-go-apiv2/peer"
)

type E2EChaincode struct{}

func (c *E2EChaincode) Init(stub shim.ChaincodeStubInterface) *pb.Response {
	return shim.Success(nil)
}

func (c *E2EChaincode) Invoke(stub shim.ChaincodeStubInterface) *pb.Response {
	fn, args := stub.GetFunctionAndParameters()

	switch strings.ToLower(fn) {
	case "get":
		if len(args) != 1 {
			return shim.Error("get expects 1 argument: key")
		}
		value, err := stub.GetState(args[0])
		if err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success(value)

	case "compatv2":
		if len(args) != 3 {
			return shim.Error("compatv2 expects 3 arguments: key value delete_key")
		}
		if err := sleepIfConfigured("E2E_COMPATV2_SLEEP"); err != nil {
			return shim.Error(err.Error())
		}
		return c.compatv2(stub, args)

	default:
		return shim.Error(fmt.Sprintf("unknown function %q", fn))
	}
}

func (c *E2EChaincode) compatv2(stub shim.ChaincodeStubInterface, args []string) *pb.Response {
	key, value, deleteKey := args[0], args[1], args[2]
	seedOldValue := "old-value"
	seedDeleteValue := "delete-me"

	committedOldValue, err := stub.GetState(key)
	if err != nil {
		return shim.Error(err.Error())
	}
	committedDeleteValue, err := stub.GetState(deleteKey)
	if err != nil {
		return shim.Error(err.Error())
	}

	if err := stub.PutState(key, []byte(seedOldValue)); err != nil {
		return shim.Error(err.Error())
	}
	oldValue, err := stub.GetState(key)
	if err != nil {
		return shim.Error(err.Error())
	}
	if err := stub.PutState(key, []byte(value)); err != nil {
		return shim.Error(err.Error())
	}
	afterPutValue, err := stub.GetState(key)
	if err != nil {
		return shim.Error(err.Error())
	}

	if err := stub.PutState(deleteKey, []byte(seedDeleteValue)); err != nil {
		return shim.Error(err.Error())
	}
	deleteOldValue, err := stub.GetState(deleteKey)
	if err != nil {
		return shim.Error(err.Error())
	}
	if err := stub.DelState(deleteKey); err != nil {
		return shim.Error(err.Error())
	}
	afterDeleteValue, err := stub.GetState(deleteKey)
	if err != nil {
		return shim.Error(err.Error())
	}

	function, params := stub.GetFunctionAndParameters()
	payloadFields := map[string]any{
		"function":                   function,
		"parameters":                 params,
		"tx_id":                      stub.GetTxID(),
		"channel_id":                 stub.GetChannelID(),
		"committed_old_value":        nullableString(committedOldValue),
		"committed_delete_old_value": nullableString(committedDeleteValue),
		"seed_old_value":             seedOldValue,
		"seed_delete_value":          seedDeleteValue,
		"old_value":                  nullableString(oldValue),
		"after_put_value":            nullableString(afterPutValue),
		"delete_old_value":           nullableString(deleteOldValue),
		"after_delete_value":         nullableString(afterDeleteValue),
	}
	if marker := os.Getenv("E2E_COMPATV2_MISMATCH"); marker != "" {
		payloadFields["mismatch_marker"] = marker
	}
	payload, err := json.Marshal(payloadFields)
	if err != nil {
		return shim.Error(err.Error())
	}

	eventPayload, err := json.Marshal(map[string]any{
		"function":    function,
		"key":         key,
		"value":       value,
		"deleted_key": deleteKey,
	})
	if err != nil {
		return shim.Error(err.Error())
	}
	if err := stub.SetEvent("compatv2-event", eventPayload); err != nil {
		return shim.Error(err.Error())
	}
	return shim.Success(payload)
}

func main() {
	ccid := flag.String("ccid", getenv("CHAINCODE_ID", "0:sample"), "chaincode ID sent during REGISTER")
	address := flag.String("address", getenv("CHAINCODE_ADDRESS", "127.0.0.1:9999"), "listen address")
	flag.Parse()

	server := &shim.ChaincodeServer{
		CCID:    *ccid,
		Address: *address,
		CC:      new(E2EChaincode),
		TLSProps: shim.TLSProperties{
			Disabled: true,
		},
	}

	if err := server.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "e2e chaincode failed: %s\n", err)
		os.Exit(1)
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
}

func sleepIfConfigured(key string) error {
	value := os.Getenv(key)
	if value == "" {
		return nil
	}
	delay, err := time.ParseDuration(value)
	if err != nil {
		return fmt.Errorf("%s must be a duration: %w", key, err)
	}
	time.Sleep(delay)
	return nil
}

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

func nullableString(value []byte) any {
	if value == nil {
		return nil
	}
	return string(value)
}
