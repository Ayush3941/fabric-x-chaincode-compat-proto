package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/api/committerpb"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"github.com/hyperledger/fabric-x-sdk/blocks"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
	efabx "github.com/hyperledger/fabric-x-sdk/endorsement/fabricx"
	"github.com/hyperledger/fabric-x-sdk/identity"
	"github.com/hyperledger/fabric-x-sdk/network"
	nfabx "github.com/hyperledger/fabric-x-sdk/network/fabricx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func main() {
	var (
		artifacts   = flag.String("artifacts", filepath.Join("artifacts"), "generated artifacts directory")
		channel     = flag.String("channel", "channelqc4", "channel ID")
		namespace   = flag.String("namespace", "0", "Fabric-X namespace")
		nsVersion   = flag.String("namespace-version", "1.0", "proposal namespace version string")
		key         = flag.String("key", "asset1", "key to write")
		value       = flag.String("value", "value1", "value to write")
		readVersion = flag.Int64("read-version", -1, "optional Fabric-X key version for read-write update; -1 means blind write")
		waitStatus  = flag.Bool("wait", false, "wait for transaction status")
		queryRow    = flag.Bool("query", false, "query key after commit/status wait")
		timeout     = flag.Duration("timeout", 2*time.Minute, "overall timeout")
		orderers    = flag.String("orderers", "127.0.0.1:6022,127.0.0.1:6122,127.0.0.1:6222,127.0.0.1:6322", "comma-separated Arma router endpoints")
		queryAddr   = flag.String("query-address", "127.0.0.1:7001", "query service endpoint")
	)
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	signer, err := identity.SignerFromMSP(
		filepath.Join(*artifacts, "peerOrganizations", "peer-org-0", "users", "client@peer-org-0", "msp"),
		"org-0",
	)
	must(err, "load signer")

	rws := blocks.ReadWriteSet{
		Writes: []blocks.KVWrite{{
			Key:   *key,
			Value: []byte(*value),
		}},
	}
	if *readVersion >= 0 {
		rws.Reads = append(rws.Reads, blocks.KVRead{
			Key:     *key,
			Version: &blocks.Version{BlockNum: uint64(*readVersion)},
		})
	}

	signedProp, err := network.NewSignedProposal(signer, *channel, *namespace, *nsVersion, [][]byte{[]byte("invoke"), []byte(*key)})
	must(err, "create signed proposal")

	inv, err := endorsement.Parse(signedProp, time.Time{})
	must(err, "parse signed proposal")

	result := endorsement.Success(rws, nil, []byte("rws-smoke"))
	resp, err := efabx.NewEndorsementBuilder(signer).Endorse(inv, result)
	must(err, "endorse Fabric-X RW set")

	submitter, err := nfabx.NewSubmitter(
		ctx,
		ordererConfs(*artifacts, *orderers),
		signer,
		0,
		sdk.NewStdLogger("rws-smoke"),
	)
	must(err, "create submitter")
	defer submitter.Close() //nolint:errcheck

	end := sdk.Endorsement{
		Proposal:  inv.Proposal,
		Responses: []*peer.ProposalResponse{resp},
	}

	must(submitter.Submit(ctx, end), "submit transaction")
	fmt.Printf("submitted tx_id=%s namespace=%s key=%s value=%s\n", inv.TxID, *namespace, *key, *value)

	if *waitStatus || *queryRow {
		conn, err := queryConn(*artifacts, *queryAddr)
		must(err, "connect query service")
		defer conn.Close() //nolint:errcheck

		client := committerpb.NewQueryServiceClient(conn)
		if *waitStatus {
			status, err := waitForStatus(ctx, client, inv.TxID)
			must(err, "wait transaction status")
			fmt.Printf("status tx_id=%s status=%s block=%d tx_num=%d\n",
				inv.TxID,
				status.Status.String(),
				status.Ref.GetBlockNum(),
				status.Ref.GetTxNum(),
			)
		}
		if *queryRow {
			must(queryKey(ctx, client, *namespace, *key), "query key")
		}
	}
}

func ordererConfs(artifacts, endpoints string) []network.OrdererConf {
	parts := strings.Split(endpoints, ",")
	confs := make([]network.OrdererConf, 0, len(parts))
	for i, endpoint := range parts {
		endpoint = strings.TrimSpace(endpoint)
		if endpoint == "" {
			continue
		}
		org := i + 1
		confs = append(confs, network.OrdererConf{
			Address: endpoint,
			TLS: network.TLSConfig{
				Mode:        network.TLSModeMTLS,
				CertPath:    filepath.Join(artifacts, "peerOrganizations", "peer-org-0", "peers", "helper.peer-org-0", "tls", "server.crt"),
				KeyPath:     filepath.Join(artifacts, "peerOrganizations", "peer-org-0", "peers", "helper.peer-org-0", "tls", "server.key"),
				CACertPaths: []string{filepath.Join(artifacts, "ordererOrganizations", "orderer-org-"+strconv.Itoa(org), "msp", "tlscacerts", "tlsca.orderer-org-"+strconv.Itoa(org)+"-cert.pem")},
				ServerName:  serverName(endpoint),
			},
		})
	}
	return confs
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

func waitForStatus(ctx context.Context, client committerpb.QueryServiceClient, txID string) (*committerpb.TxStatus, error) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			res, err := client.GetTransactionStatus(ctx, &committerpb.TxStatusQuery{TxIds: []string{txID}})
			if err != nil {
				return nil, err
			}
			if len(res.Statuses) > 0 {
				return res.Statuses[0], nil
			}
		}
	}
}

func queryKey(ctx context.Context, client committerpb.QueryServiceClient, namespace, key string) error {
	rows, err := client.GetRows(ctx, &committerpb.Query{
		Namespaces: []*committerpb.QueryNamespace{{
			NsId: namespace,
			Keys: [][]byte{[]byte(key)},
		}},
	})
	if err != nil {
		return err
	}
	for _, ns := range rows.Namespaces {
		for _, row := range ns.Rows {
			fmt.Printf("row namespace=%s key=%s value=%s version=%d\n", ns.NsId, string(row.Key), string(row.Value), row.Version)
			return nil
		}
	}
	return fmt.Errorf("key %q not found in namespace %q", key, namespace)
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
