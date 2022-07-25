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
	"io"
	"strings"
	"unsafe"

	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
	"github.com/tigrisdata/tigris-client-go/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/credentials/oauth"
	grpcmd "google.golang.org/grpc/metadata"
)

const (
	DefaultGRPCPort = 443
)

type grpcDriver struct {
	api api.TigrisClient
	driverWithOptions
	conn *grpc.ClientConn
}

func GRPCError(err error) error {
	if err == nil {
		return nil
	}
	if err == io.EOF {
		return err
	}
	return &Error{api.FromStatusError(err)}
}

// NewGRPCClient return Driver interface implementation using GRPC transport protocol
func NewGRPCClient(ctx context.Context, url string, config *config.Driver) (Driver, error) {
	token, oCfg, ctxClient := getAuthToken(ctx, config)

	ts := oCfg.TokenSource(ctxClient, token)

	if !strings.Contains(url, ":") {
		url = fmt.Sprintf("%s:%d", url, DefaultGRPCPort)
	}

	opts := []grpc.DialOption{
		grpc.FailOnNonTempDialError(true),
		grpc.WithReturnConnectionError(),
		grpc.WithUserAgent(UserAgent),
		grpc.WithBlock(),
	}

	if config.TLS != nil || token.AccessToken != "" || token.RefreshToken != "" {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(config.TLS)))

		if token.AccessToken != "" || token.RefreshToken != "" {
			opts = append(opts, grpc.WithPerRPCCredentials(oauth.TokenSource{TokenSource: ts}))
		}
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.Dial(url, opts...)
	if err != nil {
		return nil, GRPCError(err)
	}

	return &driver{driverWithOptions: &grpcDriver{conn: conn, api: api.NewTigrisClient(conn)}}, nil
}

func (c *grpcDriver) Close() error {
	if c.conn == nil {
		return nil
	}
	return GRPCError(c.conn.Close())
}

func (c *grpcDriver) UseDatabase(name string) Database {
	return &driverCRUD{&grpcCRUD{db: name, api: c.api}}
}

func (c *grpcDriver) Info(ctx context.Context) (*InfoResponse, error) {
	r, err := c.api.GetInfo(ctx, &api.GetInfoRequest{})
	if err != nil {
		return nil, GRPCError(err)
	}

	return (*InfoResponse)(r), nil
}

func (c *grpcDriver) ListDatabases(ctx context.Context) ([]string, error) {
	r, err := c.api.ListDatabases(ctx, &api.ListDatabasesRequest{})
	if err != nil {
		return nil, GRPCError(err)
	}

	var databases []string
	for _, c := range r.GetDatabases() {
		databases = append(databases, c.GetDb())
	}
	return databases, nil
}

func (c *grpcDriver) DescribeDatabase(ctx context.Context, db string) (*DescribeDatabaseResponse, error) {
	r, err := c.api.DescribeDatabase(ctx, &api.DescribeDatabaseRequest{
		Db: db,
	})
	if err != nil {
		return nil, GRPCError(err)
	}

	return (*DescribeDatabaseResponse)(r), nil
}

func (c *grpcDriver) createDatabaseWithOptions(ctx context.Context, db string, options *DatabaseOptions) error {
	_, err := c.api.CreateDatabase(ctx, &api.CreateDatabaseRequest{
		Db:      db,
		Options: (*api.DatabaseOptions)(options),
	})
	return GRPCError(err)
}

func (c *grpcDriver) dropDatabaseWithOptions(ctx context.Context, db string, options *DatabaseOptions) error {
	_, err := c.api.DropDatabase(ctx, &api.DropDatabaseRequest{
		Db:      db,
		Options: (*api.DatabaseOptions)(options),
	})
	return GRPCError(err)
}

func (c *grpcDriver) beginTxWithOptions(ctx context.Context, db string, options *TxOptions) (txWithOptions, error) {
	resp, err := c.api.BeginTransaction(ctx, &api.BeginTransactionRequest{
		Db:      db,
		Options: (*api.TransactionOptions)(options),
	})
	if err != nil {
		return nil, GRPCError(err)
	}
	if resp.GetTxCtx() == nil {
		return nil, GRPCError(fmt.Errorf("empty transaction context in response"))
	}
	return &grpcCRUD{db: db, api: c.api, txCtx: resp.GetTxCtx()}, nil
}

func setGRPCTxCtx(ctx context.Context, txCtx *api.TransactionCtx) context.Context {
	if txCtx == nil || txCtx.Id == "" {
		return ctx
	}

	return grpcmd.AppendToOutgoingContext(ctx, api.HeaderTxID, txCtx.Id, api.HeaderTxOrigin, txCtx.Origin)
}

func (c *grpcCRUD) Commit(ctx context.Context) error {
	ctx = setGRPCTxCtx(ctx, c.txCtx)

	_, err := c.api.CommitTransaction(ctx, &api.CommitTransactionRequest{
		Db: c.db,
	})

	if err = GRPCError(err); err == nil {
		c.committed = true
	}

	return err
}

func (c *grpcCRUD) Rollback(ctx context.Context) error {
	ctx = setGRPCTxCtx(ctx, c.txCtx)

	if c.committed {
		return nil
	}
	_, err := c.api.RollbackTransaction(ctx, &api.RollbackTransactionRequest{
		Db: c.db,
	})

	return GRPCError(err)
}

type grpcCRUD struct {
	db    string
	api   api.TigrisClient
	txCtx *api.TransactionCtx

	committed bool
}

func (c *grpcCRUD) listCollectionsWithOptions(ctx context.Context, options *CollectionOptions) ([]string, error) {
	ctx = setGRPCTxCtx(ctx, c.txCtx)

	r, err := c.api.ListCollections(ctx, &api.ListCollectionsRequest{
		Db:      c.db,
		Options: (*api.CollectionOptions)(options),
	})
	if err != nil {
		return nil, GRPCError(err)
	}

	var collections []string
	for _, c := range r.GetCollections() {
		collections = append(collections, c.GetCollection())
	}
	return collections, nil
}

func (c *grpcCRUD) describeCollectionWithOptions(ctx context.Context, collection string, _ *CollectionOptions) (*DescribeCollectionResponse, error) {
	r, err := c.api.DescribeCollection(ctx, &api.DescribeCollectionRequest{
		Db:         c.db,
		Collection: collection,
	})
	if err != nil {
		return nil, GRPCError(err)
	}

	return (*DescribeCollectionResponse)(r), nil
}

func (c *grpcCRUD) createOrUpdateCollectionWithOptions(ctx context.Context, collection string, schema Schema, options *CollectionOptions) error {
	ctx = setGRPCTxCtx(ctx, c.txCtx)

	_, err := c.api.CreateOrUpdateCollection(ctx, &api.CreateOrUpdateCollectionRequest{
		Db:         c.db,
		Collection: collection,
		Schema:     schema,
		Options:    (*api.CollectionOptions)(options),
	})
	return GRPCError(err)
}

func (c *grpcCRUD) dropCollectionWithOptions(ctx context.Context, collection string, options *CollectionOptions) error {
	ctx = setGRPCTxCtx(ctx, c.txCtx)
	_, err := c.api.DropCollection(ctx, &api.DropCollectionRequest{
		Db:         c.db,
		Collection: collection,
		Options:    (*api.CollectionOptions)(options),
	})
	return GRPCError(err)
}

func (c *grpcCRUD) insertWithOptions(ctx context.Context, collection string, docs []Document, options *InsertOptions) (*InsertResponse, error) {
	ctx = setGRPCTxCtx(ctx, c.txCtx)

	resp, err := c.api.Insert(ctx, &api.InsertRequest{
		Db:         c.db,
		Collection: collection,
		Documents:  *(*[][]byte)(unsafe.Pointer(&docs)),
		Options:    (*api.InsertRequestOptions)(options),
	})

	if err != nil {
		return nil, GRPCError(err)
	}

	return (*InsertResponse)(resp), nil
}

func (c *grpcCRUD) replaceWithOptions(ctx context.Context, collection string, docs []Document, options *ReplaceOptions) (*ReplaceResponse, error) {
	ctx = setGRPCTxCtx(ctx, c.txCtx)

	resp, err := c.api.Replace(ctx, &api.ReplaceRequest{
		Db:         c.db,
		Collection: collection,
		Documents:  *(*[][]byte)(unsafe.Pointer(&docs)),
		Options:    (*api.ReplaceRequestOptions)(options),
	})

	if err != nil {
		return nil, GRPCError(err)
	}

	return (*ReplaceResponse)(resp), nil
}

func (c *grpcCRUD) updateWithOptions(ctx context.Context, collection string, filter Filter, fields Update, options *UpdateOptions) (*UpdateResponse, error) {
	ctx = setGRPCTxCtx(ctx, c.txCtx)

	resp, err := c.api.Update(ctx, &api.UpdateRequest{
		Db:         c.db,
		Collection: collection,
		Filter:     filter,
		Fields:     fields,
		Options:    (*api.UpdateRequestOptions)(options),
	})

	return (*UpdateResponse)(resp), GRPCError(err)
}

func (c *grpcCRUD) deleteWithOptions(ctx context.Context, collection string, filter Filter, options *DeleteOptions) (*DeleteResponse, error) {
	ctx = setGRPCTxCtx(ctx, c.txCtx)

	resp, err := c.api.Delete(ctx, &api.DeleteRequest{
		Db:         c.db,
		Collection: collection,
		Filter:     filter,
		Options:    (*api.DeleteRequestOptions)(options),
	})

	return (*DeleteResponse)(resp), GRPCError(err)
}

func (c *grpcCRUD) readWithOptions(ctx context.Context, collection string, filter Filter, fields Projection, options *ReadOptions) (Iterator, error) {
	ctx = setGRPCTxCtx(ctx, c.txCtx)

	cctx, cancel := context.WithCancel(ctx)

	resp, err := c.api.Read(cctx, &api.ReadRequest{
		Db:         c.db,
		Collection: collection,
		Filter:     filter,
		Fields:     fields,
		Options:    (*api.ReadRequestOptions)(options),
	})
	if err != nil {
		cancel()
		return nil, GRPCError(err)
	}

	return &readIterator{streamReader: &grpcStreamReader{stream: resp, cancel: cancel}}, nil
}

type grpcStreamReader struct {
	stream api.Tigris_ReadClient
	cancel context.CancelFunc
}

func (g *grpcStreamReader) read() (Document, error) {
	resp, err := g.stream.Recv()
	if err != nil {
		return nil, GRPCError(err)
	}

	return resp.Data, nil
}

func (g *grpcStreamReader) close() error {
	g.cancel()
	return nil
}

func (c *grpcCRUD) search(ctx context.Context, collection string, req *SearchRequest) (SearchResultIterator, error) {
	cctx, cancel := context.WithCancel(ctx)

	resp, err := c.api.Search(cctx, &api.SearchRequest{
		Db:           c.db,
		Collection:   collection,
		Q:            req.Q,
		SearchFields: req.SearchFields,
		Filter:       req.Filter,
		Facet:        req.Facet,
		Fields:       req.ReadFields,
		PageSize:     req.PageSize,
		Page:         req.Page,
	})

	if err != nil {
		cancel()
		return nil, GRPCError(err)
	}

	return &searchResultIterator{
		searchStreamReader: &grpcSearchReader{stream: resp, cancel: cancel},
	}, nil
}

type grpcSearchReader struct {
	stream api.Tigris_SearchClient
	cancel context.CancelFunc
}

func (g *grpcSearchReader) read() (SearchResponse, error) {
	resp, err := g.stream.Recv()
	if err != nil {
		return nil, GRPCError(err)
	}
	return resp, nil
}

func (g *grpcSearchReader) close() error {
	g.cancel()
	return nil
}

func (c *grpcCRUD) eventsWithOptions(ctx context.Context, collection string, options *EventsOptions) (EventIterator, error) {
	cctx, cancel := context.WithCancel(ctx)

	resp, err := c.api.Events(cctx, &api.EventsRequest{
		Db:         c.db,
		Collection: collection,
		Options:    (*api.EventsRequestOptions)(options),
	})
	if err != nil {
		cancel()
		return nil, GRPCError(err)
	}

	return &eventReadIterator{eventStreamReader: &grpcEventStreamReader{stream: resp, cancel: cancel}}, nil
}

type grpcEventStreamReader struct {
	stream api.Tigris_EventsClient
	cancel context.CancelFunc
}

func (g *grpcEventStreamReader) read() (Event, error) {
	resp, err := g.stream.Recv()
	if err != nil {
		return nil, GRPCError(err)
	}

	return resp.Event, nil
}

func (g *grpcEventStreamReader) close() error {
	g.cancel()
	return nil
}

func (c *grpcCRUD) publishWithOptions(ctx context.Context, collection string, docs []Document, options *PublishOptions) (*PublishResponse, error) {
	ctx = setGRPCTxCtx(ctx, c.txCtx)

	resp, err := c.api.Publish(ctx, &api.PublishRequest{
		Db:         c.db,
		Collection: collection,
		Messages:   *(*[][]byte)(unsafe.Pointer(&docs)),
		Options:    (*api.PublishRequestOptions)(options),
	})

	if err != nil {
		return nil, GRPCError(err)
	}

	return (*PublishResponse)(resp), nil
}

type grpcSubscribeStreamReader struct {
	stream api.Tigris_SubscribeClient
}

func (g *grpcSubscribeStreamReader) read() (Document, error) {
	resp, err := g.stream.Recv()
	if err != nil {
		return nil, GRPCError(err)
	}

	return resp.Message, nil
}

func (g *grpcSubscribeStreamReader) close() error {
	return nil
}

func (c *grpcCRUD) subscribeWithOptions(ctx context.Context, collection string, options *SubscribeOptions) (Iterator, error) {
	ctx = setGRPCTxCtx(ctx, c.txCtx)

	resp, err := c.api.Subscribe(ctx, &api.SubscribeRequest{
		Db:         c.db,
		Collection: collection,
		Options:    (*api.SubscribeRequestOptions)(options),
	})
	if err != nil {
		return nil, GRPCError(err)
	}

	return &readIterator{streamReader: &grpcSubscribeStreamReader{resp}}, nil
}
