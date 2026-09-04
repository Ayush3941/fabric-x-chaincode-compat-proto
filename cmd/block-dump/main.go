package main

import (
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/api/applicationpb"
	"github.com/hyperledger/fabric-x-common/api/committerpb"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-sdk/network"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

func main() {
	var (
		artifacts = flag.String("artifacts", filepath.Join("artifacts"), "generated artifacts directory")
		queryAddr = flag.String("query-address", "127.0.0.1:4001", "sidecar BlockQueryService endpoint")
		from      = flag.Uint64("from", 0, "first block number to inspect")
		to        = flag.Int64("to", -1, "last block number to inspect; -1 means current height - 1")
		txID      = flag.String("txid", "", "inspect the block containing this transaction ID")
		rawJSON   = flag.Bool("raw-json", false, "print the raw common.Block as proto JSON")
		timeout   = flag.Duration("timeout", 30*time.Second, "request timeout")
	)
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	conn, err := queryConn(*artifacts, *queryAddr)
	must(err, "connect block query service")
	defer conn.Close() //nolint:errcheck

	client := committerpb.NewBlockQueryServiceClient(conn)
	info, err := client.GetBlockchainInfo(ctx, &emptypb.Empty{})
	must(err, "get blockchain info")

	fmt.Printf("height=%d current_hash=%s previous_hash=%s\n",
		info.Height,
		base64.StdEncoding.EncodeToString(info.CurrentBlockHash),
		base64.StdEncoding.EncodeToString(info.PreviousBlockHash),
	)

	if *txID != "" {
		block, err := client.GetBlockByTxID(ctx, &committerpb.TxID{TxId: *txID})
		must(err, "get block by txid")
		dumpBlock(block, *rawJSON)
		return
	}

	last := uint64(0)
	if info.Height > 0 {
		last = info.Height - 1
	}
	if *to >= 0 {
		last = uint64(*to)
	}
	if *from > last {
		return
	}
	for n := *from; n <= last; n++ {
		block, err := client.GetBlockByNumber(ctx, &committerpb.BlockNumber{Number: n})
		must(err, "get block "+strconv.FormatUint(n, 10))
		dumpBlock(block, *rawJSON)
	}
}

func dumpBlock(block *common.Block, rawJSON bool) {
	if rawJSON {
		out, err := protojson.MarshalOptions{Multiline: true, Indent: "  "}.Marshal(block)
		must(err, "marshal block json")
		fmt.Println(string(out))
		return
	}

	if block.GetHeader() == nil {
		fmt.Println("Block <nil header>")
		return
	}
	txCount := 0
	if block.GetData() != nil {
		txCount = len(block.Data.Data)
	}
	fmt.Printf("\nBlock %d tx_count=%d data_hash=%s previous_hash=%s\n",
		block.Header.Number,
		txCount,
		base64.StdEncoding.EncodeToString(block.Header.DataHash),
		base64.StdEncoding.EncodeToString(block.Header.PreviousHash),
	)

	if block.GetMetadata() != nil && len(block.Metadata.Metadata) > int(common.BlockMetadataIndex_TRANSACTIONS_FILTER) {
		filter := block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER]
		if len(filter) > 0 {
			fmt.Printf("  tx_filter=%v\n", []byte(filter))
		}
	}

	if block.GetData() == nil {
		return
	}
	for i, envBytes := range block.Data.Data {
		dumpEnvelope(i, envBytes)
	}
}

func dumpEnvelope(i int, envBytes []byte) {
	var env common.Envelope
	if err := proto.Unmarshal(envBytes, &env); err != nil {
		fmt.Printf("  [%d] malformed envelope: %v\n", i, err)
		return
	}
	payload, err := protoutil.UnmarshalPayload(env.Payload)
	if err != nil {
		fmt.Printf("  [%d] malformed payload: %v\n", i, err)
		return
	}
	if payload.GetHeader() == nil {
		fmt.Printf("  [%d] payload has nil header\n", i)
		return
	}
	chdr, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		fmt.Printf("  [%d] malformed channel header: %v\n", i, err)
		return
	}

	typeName := common.HeaderType_name[chdr.Type]
	if typeName == "" {
		typeName = "UNKNOWN"
	}
	fmt.Printf("  [%d] Envelope type=%s/%d channel=%s tx_id=%s payload_bytes=%d signature_bytes=%d\n",
		i,
		typeName,
		chdr.Type,
		chdr.ChannelId,
		chdr.TxId,
		len(payload.Data),
		len(env.Signature),
	)

	switch common.HeaderType(chdr.Type) {
	case common.HeaderType_CONFIG:
		dumpConfig(payload.Data)
	case common.HeaderType_CONFIG_UPDATE:
		dumpConfigUpdate(payload.Data)
	case common.HeaderType_ENDORSER_TRANSACTION:
		dumpEndorserTx(payload.Data)
	case common.HeaderType_MESSAGE:
		dumpFabricXTx(payload.Data)
	default:
		fmt.Println("    data: unsupported/opaque payload type")
	}
}

func dumpConfig(data []byte) {
	var ce common.ConfigEnvelope
	if err := proto.Unmarshal(data, &ce); err != nil {
		fmt.Printf("    data: ConfigEnvelope decode failed: %v\n", err)
		return
	}
	seq := uint64(0)
	if ce.GetConfig() != nil {
		seq = ce.Config.Sequence
	}
	fmt.Printf("    data: ConfigEnvelope sequence=%d last_update_present=%t\n", seq, ce.GetLastUpdate() != nil)
}

func dumpConfigUpdate(data []byte) {
	var cue common.ConfigUpdateEnvelope
	if err := proto.Unmarshal(data, &cue); err != nil {
		fmt.Printf("    data: ConfigUpdateEnvelope decode failed: %v\n", err)
		return
	}
	var cu common.ConfigUpdate
	if err := proto.Unmarshal(cue.ConfigUpdate, &cu); err != nil {
		fmt.Printf("    data: ConfigUpdateEnvelope signatures=%d config_update_bytes=%d\n", len(cue.Signatures), len(cue.ConfigUpdate))
		return
	}
	fmt.Printf("    data: ConfigUpdateEnvelope channel=%s signatures=%d\n", cu.ChannelId, len(cue.Signatures))
}

func dumpEndorserTx(data []byte) {
	var tx peer.Transaction
	if err := proto.Unmarshal(data, &tx); err != nil {
		fmt.Printf("    data: peer.Transaction decode failed: %v\n", err)
		return
	}
	fmt.Printf("    data: peer.Transaction actions=%d\n", len(tx.Actions))
}

func dumpFabricXTx(data []byte) {
	var tx applicationpb.Tx
	if err := proto.Unmarshal(data, &tx); err != nil {
		fmt.Printf("    data: applicationpb.Tx decode failed: %v\n", err)
		return
	}
	fmt.Printf("    data: applicationpb.Tx namespaces=%d endorsements=%d metadata=%d\n",
		len(tx.Namespaces),
		len(tx.Endorsements),
		len(tx.Metadata),
	)
	for idx, endorsements := range tx.Endorsements {
		if endorsements == nil {
			fmt.Printf("      endorsements[%d] signers=0\n", idx)
			continue
		}
		signers := endorsements.GetEndorsementsWithIdentity()
		fmt.Printf("      endorsements[%d] signers=%d\n", idx, len(signers))
		for signerIdx, signer := range signers {
			mspID := "<nil>"
			if signer.GetIdentity() != nil {
				mspID = signer.GetIdentity().GetMspId()
			}
			fmt.Printf("        signer[%d] msp_id=%s signature_bytes=%d\n",
				signerIdx,
				mspID,
				len(signer.GetEndorsement()),
			)
		}
	}
	if len(tx.Metadata) > 1 && len(tx.Metadata[1]) > 0 {
		var event peer.ChaincodeEvent
		if err := proto.Unmarshal(tx.Metadata[1], &event); err != nil {
			fmt.Printf("      event decode failed: %v\n", err)
		} else {
			fmt.Printf("      event chaincode_id=%s tx_id=%s name=%s payload=%s\n",
				event.ChaincodeId,
				event.TxId,
				event.EventName,
				renderBytes(event.Payload),
			)
		}
	}
	for nsIdx, ns := range tx.Namespaces {
		fmt.Printf("      ns[%d] id=%s version=%d reads_only=%d read_writes=%d blind_writes=%d\n",
			nsIdx,
			ns.NsId,
			ns.NsVersion,
			len(ns.ReadsOnly),
			len(ns.ReadWrites),
			len(ns.BlindWrites),
		)
		for _, r := range ns.ReadsOnly {
			fmt.Printf("        read key=%s version=%s\n", renderBytes(r.Key), renderVersion(r.Version))
		}
		for _, rw := range ns.ReadWrites {
			fmt.Printf("        read_write key=%s version=%s value=%s\n", renderBytes(rw.Key), renderVersion(rw.Version), renderBytes(rw.Value))
		}
		for _, w := range ns.BlindWrites {
			fmt.Printf("        blind_write key=%s value=%s\n", renderBytes(w.Key), renderBytes(w.Value))
		}
	}
}

func renderVersion(v *uint64) string {
	if v == nil {
		return "<nil>"
	}
	return strconv.FormatUint(*v, 10)
}

func renderBytes(b []byte) string {
	if b == nil {
		return "<nil>"
	}
	if len(b) == 0 {
		return "\"\""
	}
	if utf8.Valid(b) && strings.IndexFunc(string(b), func(r rune) bool {
		return r < 32 || r == 127
	}) == -1 {
		return strconv.Quote(string(b))
	}
	return "base64:" + base64.StdEncoding.EncodeToString(b)
}

func queryConn(artifacts, address string) (*grpc.ClientConn, error) {
	tlsCfg, err := (network.TLSConfig{
		Mode:        network.TLSModeMTLS,
		CertPath:    filepath.Join(artifacts, "peerOrganizations", "peer-org-0", "peers", "helper.peer-org-0", "tls", "server.crt"),
		KeyPath:     filepath.Join(artifacts, "peerOrganizations", "peer-org-0", "peers", "helper.peer-org-0", "tls", "server.key"),
		CACertPaths: []string{filepath.Join(artifacts, "peerOrganizations", "peer-org-0", "msp", "tlscacerts", "tlsca.peer-org-0-cert.pem")},
		ServerName:  serverName(address),
	}).LoadClientTLSConfig(serverName(address))
	if err != nil {
		return nil, err
	}
	return grpc.NewClient(address, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
}

func serverName(address string) string {
	host, _, err := net.SplitHostPort(address)
	if err != nil || host == "" {
		return "127.0.0.1"
	}
	return host
}

func must(err error, step string) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %s: %v\n", step, err)
		os.Exit(1)
	}
}
