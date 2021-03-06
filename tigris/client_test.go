package tigris

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
	"github.com/tigrisdata/tigris-client-go/config"
	"github.com/tigrisdata/tigris-client-go/test"
)

func TestClient(t *testing.T) {
	mc, cancel := test.SetupTests(t, 8)
	defer cancel()

	ctx, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel1()

	cfg := &config.Database{Driver: config.Driver{URL: test.GRPCURL(8)}}
	cfg.TLS = test.SetupTLS(t)

	type Coll1 struct {
		Key1 string `tigris:"primary_key"`
	}

	txCtx := &api.TransactionCtx{Id: "tx_id1", Origin: "origin_id1"}

	mc.EXPECT().CreateDatabase(gomock.Any(),
		pm(&api.CreateDatabaseRequest{
			Db:      "db1",
			Options: &api.DatabaseOptions{},
		})).Return(&api.CreateDatabaseResponse{}, nil)

	mc.EXPECT().BeginTransaction(gomock.Any(),
		pm(&api.BeginTransactionRequest{
			Db:      "db1",
			Options: &api.TransactionOptions{},
		})).Return(&api.BeginTransactionResponse{TxCtx: txCtx}, nil)

	mc.EXPECT().CreateOrUpdateCollection(gomock.Any(),
		pm(&api.CreateOrUpdateCollectionRequest{
			Db: "db1", Collection: "coll_1",
			Schema:  []byte(`{"title":"coll_1","properties":{"Key1":{"type":"string"}},"primary_key":["Key1"]}`),
			Options: &api.CollectionOptions{},
		})).Do(func(ctx context.Context, r *api.CreateOrUpdateCollectionRequest) {
	}).Return(&api.CreateOrUpdateCollectionResponse{}, nil)

	mc.EXPECT().CommitTransaction(gomock.Any(),
		pm(&api.CommitTransactionRequest{
			Db: "db1",
		})).Return(&api.CommitTransactionResponse{}, nil)

	c, err := NewClient(ctx, cfg)
	require.NoError(t, err)

	db, err := c.OpenDatabase(ctx, "db1", &Coll1{})
	require.NoError(t, err)
	require.NotNil(t, db)

	_, err = c.OpenDatabase(setTxCtx(ctx, &Tx{}), "db1", &Coll1{})
	require.Error(t, err)

	mc.EXPECT().DropDatabase(gomock.Any(),
		pm(&api.DropDatabaseRequest{
			Db:      "db1",
			Options: &api.DatabaseOptions{},
		})).Return(&api.DropDatabaseResponse{}, nil)

	err = c.DropDatabase(ctx, "db1")
	require.NoError(t, err)

	err = c.DropDatabase(setTxCtx(ctx, &Tx{}), "db1")
	require.Error(t, err)

	err = c.Close()
	require.NoError(t, err)

	cfg.URL = "http://invalid"
	_, err = NewClient(ctx, cfg)
	require.Error(t, err)
}
