/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/hyperledger/fabric-chaincode-go/v2/shim"
	pb "github.com/hyperledger/fabric-protos-go-apiv2/peer"
)

type SimpleKVChaincode struct{}

func (c *SimpleKVChaincode) Init(stub shim.ChaincodeStubInterface) *pb.Response {
	return shim.Success(nil)
}

func (c *SimpleKVChaincode) Invoke(stub shim.ChaincodeStubInterface) *pb.Response {
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

	case "put":
		if len(args) != 2 {
			return shim.Error("put expects 2 arguments: key value")
		}
		if err := stub.PutState(args[0], []byte(args[1])); err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success([]byte(args[1]))

	case "putget":
		if len(args) != 2 {
			return shim.Error("putget expects 2 arguments: key value")
		}
		oldValue, err := stub.GetState(args[0])
		if err != nil {
			return shim.Error(err.Error())
		}
		if err := stub.PutState(args[0], []byte(args[1])); err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success(oldValue)

	case "del":
		if len(args) != 1 {
			return shim.Error("del expects 1 argument: key")
		}
		if err := stub.DelState(args[0]); err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success(nil)

	case "context":
		function, params := stub.GetFunctionAndParameters()
		payload, err := json.Marshal(map[string]any{
			"args":        byteArgsToStrings(stub.GetArgs()),
			"string_args": stub.GetStringArgs(),
			"function":    function,
			"parameters":  params,
			"tx_id":       stub.GetTxID(),
			"channel_id":  stub.GetChannelID(),
		})
		if err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success(payload)

	case "compatv1":
		if len(args) != 3 {
			return shim.Error("compatv1 expects 3 arguments: key value delete_key")
		}
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

		compositeKey, err := stub.CreateCompositeKey("compatv1", []string{key, deleteKey})
		if err != nil {
			return shim.Error(err.Error())
		}
		splitObjectType, splitAttributes, err := stub.SplitCompositeKey(compositeKey)
		if err != nil {
			return shim.Error(err.Error())
		}

		function, params := stub.GetFunctionAndParameters()
		payload, err := json.Marshal(map[string]any{
			"args":                       byteArgsToStrings(stub.GetArgs()),
			"string_args":                stub.GetStringArgs(),
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
			"composite_key":              compositeKey,
			"split_object_type":          splitObjectType,
			"split_attributes":           splitAttributes,
			"ok_status":                  shim.OK,
			"error_status":               shim.ERROR,
		})
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
		if err := stub.SetEvent("compatv1-event", eventPayload); err != nil {
			return shim.Error(err.Error())
		}
		return shim.Success(payload)

	case "compatv2":
		if len(args) != 3 {
			return shim.Error("compatv2 expects 3 arguments: key value delete_key")
		}
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

		compositeKey, err := stub.CreateCompositeKey("compatv2", []string{key, deleteKey})
		if err != nil {
			return shim.Error(err.Error())
		}
		splitObjectType, splitAttributes, err := stub.SplitCompositeKey(compositeKey)
		if err != nil {
			return shim.Error(err.Error())
		}

		function, params := stub.GetFunctionAndParameters()
		payload, err := json.Marshal(map[string]any{
			"args":                       byteArgsToStrings(stub.GetArgs()),
			"string_args":                stub.GetStringArgs(),
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
			"composite_key":              compositeKey,
			"split_object_type":          splitObjectType,
			"split_attributes":           splitAttributes,
			"ok_status":                  shim.OK,
			"error_status":               shim.ERROR,
		})
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

	case "compaterror":
		return shim.Error("compat error requested")

	default:
		return shim.Error(fmt.Sprintf("unknown function %q", fn))
	}
}

func main() {
	ccid := flag.String("ccid", getenv("CHAINCODE_ID", "0:sample"), "chaincode ID sent during REGISTER")
	address := flag.String("address", getenv("CHAINCODE_ADDRESS", "127.0.0.1:9999"), "listen address")
	flag.Parse()

	server := &shim.ChaincodeServer{
		CCID:    *ccid,
		Address: *address,
		CC:      new(SimpleKVChaincode),
		TLSProps: shim.TLSProperties{
			Disabled: true,
		},
	}

	if err := server.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "sample external chaincode failed: %s\n", err)
		os.Exit(1)
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
}

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

func byteArgsToStrings(args [][]byte) []string {
	res := make([]string, 0, len(args))
	for _, arg := range args {
		res = append(res, string(arg))
	}
	return res
}

func nullableString(value []byte) any {
	if value == nil {
		return nil
	}
	return string(value)
}
