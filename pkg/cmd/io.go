package cmd

import (
	"context"
	"log"
	"math/rand"
	"reflect"
	"time"

	cerrors "github.com/drausin/libri/libri/common/errors"
	lserver "github.com/drausin/libri/libri/common/logging"
	"github.com/drausin/libri/libri/librarian/api"
	"github.com/drausin/libri/libri/librarian/server"
	"github.com/elxirhealth/courier/pkg/courierapi"
	server2 "github.com/elxirhealth/service-base/pkg/server"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

const (
	nDocsFlag    = "nDocs"
	timeoutFlag  = "timeout"
	logKey       = "key"
	logOperation = "operation"
)

var ioCmd = &cobra.Command{
	Use:   "io",
	Short: "test input/output of one or more courier servers",
	Run: func(cmd *cobra.Command, args []string) {
		if err := testIO(); err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	testCmd.AddCommand(ioCmd)

	ioCmd.Flags().Uint(nDocsFlag, 8,
		"number of documents to put and then get from the courier")
	ioCmd.Flags().Uint(timeoutFlag, 3,
		"timeout (secs) of courier requests")

	// bind viper flags
	viper.SetEnvPrefix(envVarPrefix) // look for env vars with "LIBRI_" prefix
	viper.AutomaticEnv()             // read in environment variables that match
	cerrors.MaybePanic(viper.BindPFlags(ioCmd.Flags()))
}

func testIO() error {
	rng := rand.New(rand.NewSource(0))
	logger := lserver.NewDevLogger(lserver.GetLogLevel(viper.GetString(logLevelFlag)))
	addrs, err := server.ParseAddrs(viper.GetStringSlice(couriersFlag))
	if err != nil {
		return err
	}
	timeout := time.Duration(viper.GetInt(timeoutFlag) * 1e9)
	nDocs := viper.GetInt(nDocsFlag)

	dialer := server2.NewInsecureDialer()
	courierClients := make([]courierapi.CourierClient, len(addrs))
	for i, addr := range addrs {
		conn, err2 := dialer.Dial(addr.String())
		if err != nil {
			return err2
		}
		courierClients[i] = courierapi.NewCourierClient(conn)
	}

	docs := make([]*api.Document, nDocs)
	for i := 0; i < nDocs; i++ {
		value, key := api.NewTestDocument(rng)
		docs[i] = value

		c := courierClients[rng.Int31n(int32(len(courierClients)))]
		rq := &courierapi.PutRequest{Key: key.Bytes(), Value: value}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		rp, err2 := c.Put(ctx, rq)
		cancel()
		if err2 != nil {
			logger.Error("document put failed", zap.Error(err))
			continue
		}
		logger.Info("document put succeeded",
			zap.String(logKey, key.String()),
			zap.String(logOperation, rp.Operation.String()),
		)
	}

	for i := 0; i < nDocs; i++ {
		key, err2 := api.GetKey(docs[i])
		if err2 != nil {
			return err2
		}

		c := courierClients[rng.Int31n(int32(len(courierClients)))]
		rq := &courierapi.GetRequest{Key: key.Bytes()}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		rp, err2 := c.Get(ctx, rq)
		cancel()
		if err2 != nil {
			logger.Error("document get failed", zap.Error(err2))
			continue
		}
		if !reflect.DeepEqual(docs[i], rp.Value) {
			log.Printf("expected: %v\n", docs[i])
			log.Printf("actual: %v\n", rp.Value)
			return errors.New("gotten value does not equal expected value")
		}
		logger.Info("document get succeeded", zap.String(logKey, key.String()))
	}
	return nil
}
