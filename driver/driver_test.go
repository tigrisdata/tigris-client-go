// Copyright 2022 Tigris Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package driver

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/golang/mock/gomock"
	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigrisdb-client-go/api/server/v1"
	"github.com/tigrisdata/tigrisdb-client-go/config"
	"github.com/tigrisdata/tigrisdb-client-go/mock"
	"github.com/tigrisdata/tigrisdb-client-go/test"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func SetupGRPCTests(t *testing.T, config *config.Config) (Driver, *mock.MockTigrisDBServer, func()) {
	mockServer, cancel := test.SetupTests(t)
	config.TLS = test.SetupTLS(t)
	client, err := NewGRPCClient(context.Background(), test.GRPCURL, config)
	require.NoError(t, err)

	return client, mockServer, func() { cancel(); _ = client.Close() }
}

func SetupHTTPTests(t *testing.T, config *config.Config) (Driver, *mock.MockTigrisDBServer, func()) {
	mockServer, cancel := test.SetupTests(t)
	config.TLS = test.SetupTLS(t)
	client, err := NewHTTPClient(context.Background(), test.HTTPURL, config)
	require.NoError(t, err)

	//FIXME: implement proper wait for HTTP server to start
	time.Sleep(10 * time.Millisecond)

	return client, mockServer, func() { cancel(); _ = client.Close() }
}

func testError(t *testing.T, d Driver, mc *mock.MockTigrisDBServer, in error, exp error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var r *api.DeleteResponse
	if in == nil {
		r = &api.DeleteResponse{}
	}
	mc.EXPECT().Delete(gomock.Any(),
		pm(&api.DeleteRequest{
			Db:         "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Options:    &api.DeleteRequestOptions{WriteOptions: &api.WriteOptions{}},
		})).Return(r, in)

	_, err := d.Delete(ctx, "db1", "c1", Filter(`{"filter":"value"}`), &DeleteOptions{})

	require.Equal(t, exp, err)
}

func testErrors(t *testing.T, d Driver, mc *mock.MockTigrisDBServer) {
	cases := []struct {
		name string
		in   error
		exp  error
	}{
		{"tigrisdb_error", &api.TigrisDBError{Code: codes.Unauthenticated, Message: "some error"},
			&api.TigrisDBError{Code: codes.Unauthenticated, Message: "some error"}},
		{"error", fmt.Errorf("some error 1"),
			&api.TigrisDBError{Code: codes.Unknown, Message: "some error 1"}},
		{"grpc_error", status.Error(codes.PermissionDenied, "some error 1"),
			&api.TigrisDBError{Code: codes.PermissionDenied, Message: "some error 1"}},
		{"no_error", nil, nil},
	}

	for _, c := range cases {
		testError(t, d, mc, c.in, c.exp)
	}
}

func TestGRPCError(t *testing.T) {
	client, mockServer, cancel := SetupGRPCTests(t, &config.Config{Token: "aaa"})
	defer cancel()
	testErrors(t, client, mockServer)
}

func TestHTTPError(t *testing.T) {
	client, mockServer, cancel := SetupHTTPTests(t, &config.Config{Token: "aaa"})
	defer cancel()
	testErrors(t, client, mockServer)
}

func pm(m proto.Message) gomock.Matcher {
	return &test.ProtoMatcher{Message: m}
}

func testTxCRUDBasic(t *testing.T, c Tx, mc *mock.MockTigrisDBServer) {
	ctx := context.TODO()

	doc1 := []Document{Document(`{"K1":"vK1","K2":1,"D1":"vD1"}`)}

	options := &api.WriteOptions{}
	setGRPCTxCtx(&api.TransactionCtx{Id: "tx_id1", Origin: "origin_id1"}, options)

	mc.EXPECT().Insert(gomock.Any(),
		pm(&api.InsertRequest{
			Db:         "db1",
			Collection: "c1",
			Documents:  *(*[][]byte)(unsafe.Pointer(&doc1)),
			Options:    &api.InsertRequestOptions{WriteOptions: options},
		})).Return(&api.InsertResponse{}, nil)

	_, err := c.Insert(ctx, "c1", doc1, &InsertOptions{WriteOptions: options})
	require.NoError(t, err)

	doc123 := []Document{Document(`{"K1":"vK1","K2":1,"D1":"vD1"}`), Document(`{"K1":"vK1","K2":2,"D1":"vD2"}`), Document(`{"K1":"vK2","K2":1,"D1":"vD3"}`)}

	mc.EXPECT().Insert(gomock.Any(),
		pm(&api.InsertRequest{
			Db:         "db1",
			Collection: "c1",
			Documents:  *(*[][]byte)(unsafe.Pointer(&doc123)),
			Options:    &api.InsertRequestOptions{WriteOptions: options},
		})).Return(&api.InsertResponse{}, nil)

	_, err = c.Insert(ctx, "c1", doc123, &InsertOptions{})
	require.NoError(t, err)

	mc.EXPECT().Update(gomock.Any(),
		pm(&api.UpdateRequest{
			Db:         "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Fields:     []byte(`{"fields":1}`),
			Options:    &api.UpdateRequestOptions{WriteOptions: options},
		})).Return(&api.UpdateResponse{}, nil)

	_, err = c.Update(ctx, "c1", Filter(`{"filter":"value"}`), Fields(`{"fields":1}`), &UpdateOptions{})
	require.NoError(t, err)

	roptions := &api.ReadRequestOptions{}
	roptions.TxCtx = &api.TransactionCtx{Id: "tx_id1", Origin: "origin_id1"}

	mc.EXPECT().Read(
		pm(&api.ReadRequest{
			Db:         "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Options:    roptions,
		}), gomock.Any()).Return(nil)

	it, err := c.Read(ctx, "c1", Filter(`{"filter":"value"}`), &ReadOptions{})
	require.NoError(t, err)

	require.False(t, it.Next(nil))

	mc.EXPECT().Delete(gomock.Any(),
		pm(&api.DeleteRequest{
			Db:         "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Options:    &api.DeleteRequestOptions{WriteOptions: options},
		})).Return(&api.DeleteResponse{}, nil)

	_, err = c.Delete(ctx, "c1", Filter(`{"filter":"value"}`), &DeleteOptions{})
	require.NoError(t, err)
}

func testCRUDBasic(t *testing.T, c Driver, mc *mock.MockTigrisDBServer) {
	ctx := context.TODO()

	doc1 := []Document{Document(`{"K1":"vK1","K2":1,"D1":"vD1"}`)}

	options := &api.WriteOptions{}

	mc.EXPECT().Insert(gomock.Any(),
		pm(&api.InsertRequest{
			Db:         "db1",
			Collection: "c1",
			Documents:  *(*[][]byte)(unsafe.Pointer(&doc1)),
			Options:    &api.InsertRequestOptions{WriteOptions: options},
		})).Return(&api.InsertResponse{}, nil)

	_, err := c.Insert(ctx, "db1", "c1", doc1, &InsertOptions{WriteOptions: options})
	require.NoError(t, err)

	doc123 := []Document{Document(`{"K1":"vK1","K2":1,"D1":"vD1"}`), Document(`{"K1":"vK1","K2":2,"D1":"vD2"}`), Document(`{"K1":"vK2","K2":1,"D1":"vD3"}`)}

	mc.EXPECT().Insert(gomock.Any(),
		pm(&api.InsertRequest{
			Db:         "db1",
			Collection: "c1",
			Documents:  *(*[][]byte)(unsafe.Pointer(&doc123)),
			Options:    &api.InsertRequestOptions{WriteOptions: options},
		})).Return(&api.InsertResponse{}, nil)

	_, err = c.Insert(ctx, "db1", "c1", doc123, &InsertOptions{})
	require.NoError(t, err)

	mc.EXPECT().Update(gomock.Any(),
		pm(&api.UpdateRequest{
			Db:         "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Fields:     []byte(`{"fields":1}`),
			Options:    &api.UpdateRequestOptions{WriteOptions: options},
		})).Return(&api.UpdateResponse{}, nil)

	_, err = c.Update(ctx, "db1", "c1", Filter(`{"filter":"value"}`), Fields(`{"fields":1}`), &UpdateOptions{})
	require.NoError(t, err)

	roptions := &api.ReadRequestOptions{}

	mc.EXPECT().Read(
		pm(&api.ReadRequest{
			Db:         "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Options:    roptions,
		}), gomock.Any()).Return(nil)

	it, err := c.Read(ctx, "db1", "c1", Filter(`{"filter":"value"}`), &ReadOptions{})
	require.NoError(t, err)

	require.False(t, it.Next(nil))

	mc.EXPECT().Delete(gomock.Any(),
		pm(&api.DeleteRequest{
			Db:         "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Options:    &api.DeleteRequestOptions{WriteOptions: options},
		})).Return(&api.DeleteResponse{}, nil)

	_, err = c.Delete(ctx, "db1", "c1", Filter(`{"filter":"value"}`), &DeleteOptions{})
	require.NoError(t, err)
}

func testDriverBasic(t *testing.T, c Driver, mc *mock.MockTigrisDBServer) {
	ctx := context.TODO()

	// Test empty list response
	mc.EXPECT().ListDatabases(gomock.Any(),
		pm(&api.ListDatabasesRequest{})).Return(&api.ListDatabasesResponse{Dbs: nil}, nil)

	dbs, err := c.ListDatabases(ctx)
	require.NoError(t, err)
	require.Equal(t, []string(nil), dbs)

	mc.EXPECT().ListDatabases(gomock.Any(),
		pm(&api.ListDatabasesRequest{})).Return(&api.ListDatabasesResponse{Dbs: []string{"ldb1", "ldb2"}}, nil)

	dbs, err = c.ListDatabases(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"ldb1", "ldb2"}, dbs)

	// Test empty list response
	mc.EXPECT().ListCollections(gomock.Any(),
		pm(&api.ListCollectionsRequest{
			Db: "db1",
		})).Return(&api.ListCollectionsResponse{Collections: nil}, nil)

	colls, err := c.ListCollections(ctx, "db1")
	require.NoError(t, err)
	require.Equal(t, []string(nil), colls)

	mc.EXPECT().ListCollections(gomock.Any(),
		pm(&api.ListCollectionsRequest{
			Db: "db1",
		})).Return(&api.ListCollectionsResponse{Collections: []string{"lc1", "lc2"}}, nil)

	colls, err = c.ListCollections(ctx, "db1")
	require.NoError(t, err)
	require.Equal(t, []string{"lc1", "lc2"}, colls)

	mc.EXPECT().CreateDatabase(gomock.Any(),
		pm(&api.CreateDatabaseRequest{
			Db:      "db1",
			Options: &api.DatabaseOptions{},
		})).Return(&api.CreateDatabaseResponse{}, nil)

	err = c.CreateDatabase(ctx, "db1", &DatabaseOptions{})
	require.NoError(t, err)

	sch := `{"schema":"field"}`
	mc.EXPECT().CreateCollection(gomock.Any(),
		pm(&api.CreateCollectionRequest{
			Db:         "db1",
			Collection: "c1",
			Schema:     []byte(sch),
			Options:    &api.CollectionOptions{},
		})).Return(&api.CreateCollectionResponse{}, nil)

	err = c.CreateCollection(ctx, "db1", "c1", Schema(sch), &CollectionOptions{})
	require.NoError(t, err)

	testCRUDBasic(t, c, mc)
}

func testTxBasic(t *testing.T, c Driver, mc *mock.MockTigrisDBServer) {
	ctx := context.TODO()

	txCtx := &api.TransactionCtx{Id: "tx_id1", Origin: "origin_id1"}

	mc.EXPECT().BeginTransaction(gomock.Any(),
		pm(&api.BeginTransactionRequest{
			Db:      "db1",
			Options: &api.TransactionOptions{},
		})).Return(&api.BeginTransactionResponse{TxCtx: txCtx}, nil)

	tx, err := c.BeginTx(ctx, "db1", &TxOptions{})
	require.NoError(t, err)

	testTxCRUDBasic(t, tx, mc)

	mc.EXPECT().CommitTransaction(gomock.Any(),
		pm(&api.CommitTransactionRequest{
			Db:    "db1",
			TxCtx: txCtx,
		})).Return(&api.CommitTransactionResponse{}, nil)

	err = tx.Commit(ctx)
	require.NoError(t, err)

	mc.EXPECT().RollbackTransaction(gomock.Any(),
		pm(&api.RollbackTransactionRequest{
			Db:    "db1",
			TxCtx: txCtx,
		})).Return(&api.RollbackTransactionResponse{}, nil)

	err = tx.Rollback(ctx)
	require.NoError(t, err)
}

func TestGRPCDriver(t *testing.T) {
	client, mockServer, cancel := SetupGRPCTests(t, &config.Config{Token: "aaa"})
	defer cancel()
	testDriverBasic(t, client, mockServer)
}

func TestHTTPDriver(t *testing.T) {
	client, mockServer, cancel := SetupHTTPTests(t, &config.Config{Token: "aaa"})
	defer cancel()
	testDriverBasic(t, client, mockServer)
}

func TestTxGRPCDriver(t *testing.T) {
	client, mockServer, cancel := SetupGRPCTests(t, &config.Config{Token: "aaa"})
	defer cancel()
	testTxBasic(t, client, mockServer)
}

func TestTxHTTPDriver(t *testing.T) {
	client, mockServer, cancel := SetupHTTPTests(t, &config.Config{Token: "aaa"})
	defer cancel()
	testTxBasic(t, client, mockServer)
}

func testDriverAuth(t *testing.T, d Driver, mc *mock.MockTigrisDBServer, token string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	mc.EXPECT().Delete(gomock.Any(),
		pm(&api.DeleteRequest{
			Db:         "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Options:    &api.DeleteRequestOptions{WriteOptions: &api.WriteOptions{}},
		})).Do(func(ctx context.Context, r *api.DeleteRequest) {
		assert.Equal(t, "Bearer "+token, metautils.ExtractIncoming(ctx).Get("authorization"))
		ua := "grpcgateway-user-agent"
		if metautils.ExtractIncoming(ctx).Get(ua) == "" {
			ua = "user-agent"
		}
		assert.True(t, strings.Contains(metautils.ExtractIncoming(ctx).Get(ua), UserAgent))
	}).Return(&api.DeleteResponse{}, nil)

	_, err := d.Delete(ctx, "db1", "c1", Filter(`{"filter":"value"}`), &DeleteOptions{})
	require.NoError(t, err)
}

func TestGRPCDriverAuth(t *testing.T) {
	t.Run("config", func(t *testing.T) {
		client, mockServer, cancel := SetupGRPCTests(t, &config.Config{Token: "token_config_123"})
		defer cancel()

		testDriverAuth(t, client, mockServer, "token_config_123")
	})

	t.Run("env", func(t *testing.T) {
		err := os.Setenv(TokenEnv, "token_env_567")
		require.NoError(t, err)

		client, mockServer, cancel := SetupGRPCTests(t, &config.Config{})
		defer cancel()

		testDriverAuth(t, client, mockServer, "token_env_567")

		err = os.Unsetenv(TokenEnv)
		require.NoError(t, err)
	})
}

func TestHTTPDriverAuth(t *testing.T) {
	t.Run("config", func(t *testing.T) {
		client, mockServer, cancel := SetupHTTPTests(t, &config.Config{Token: "token_config_123"})
		defer cancel()

		testDriverAuth(t, client, mockServer, "token_config_123")
	})

	t.Run("env", func(t *testing.T) {
		err := os.Setenv(TokenEnv, "token_env_567")
		require.NoError(t, err)

		client, mockServer, cancel := SetupHTTPTests(t, &config.Config{})
		defer cancel()

		testDriverAuth(t, client, mockServer, "token_env_567")

		err = os.Unsetenv(TokenEnv)
		require.NoError(t, err)
	})
}

func TestGRPCTokenRefresh(t *testing.T) {
	ToekenRefreshURL = test.HTTPURL + "/token"

	client, mockServer, cancel := SetupGRPCTests(t, &config.Config{Token: "token_config_123:refresh_token_123"})
	defer cancel()

	testDriverAuth(t, client, mockServer, "refreshed_token_config_123")
}

func TestHTTPTokenRefresh(t *testing.T) {
	ToekenRefreshURL = test.HTTPURL + "/token"

	client, mockServer, cancel := SetupHTTPTests(t, &config.Config{Token: "token_config_123:refresh_token_123"})
	defer cancel()

	testDriverAuth(t, client, mockServer, "refreshed_token_config_123")
}
