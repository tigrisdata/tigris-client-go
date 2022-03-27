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
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"unsafe"

	apiHTTP "github.com/tigrisdata/tigrisdb-client-go/api/client/v1/api"
	api "github.com/tigrisdata/tigrisdb-client-go/api/server/v1"
	"github.com/tigrisdata/tigrisdb-client-go/config"
	"google.golang.org/grpc/codes"
)

const (
	DefaultHTTPPort = 443
)

func HTTPError(err error, resp *http.Response) error {
	if err != nil {
		if err == io.EOF {
			return err
		}
		return &api.TigrisDBError{Code: codes.Unknown, Message: err.Error()}
	}

	if resp == nil {
		return nil
	}

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
		return nil
	}

	defer func() {
		if resp != nil {
			_ = resp.Body.Close()
		}
	}()

	if !strings.Contains(resp.Header.Get("Content-Type"), "json") {
		b, _ := ioutil.ReadAll(resp.Body)
		return &api.TigrisDBError{Code: codes.Code(resp.StatusCode), Message: string(b)}
	}

	var e api.TigrisDBError
	if err := respDecode(resp.Body, &e); err != nil {
		return err
	}

	return &e
}

type httpDriver struct {
	*httpCRUD
}

func respDecode(body io.ReadCloser, v interface{}) error {
	defer func() {
		_ = body.Close()
	}()

	if err := json.NewDecoder(body).Decode(v); err != nil {
		return &api.TigrisDBError{Code: codes.Unknown, Message: err.Error()}
	}
	return nil
}

func setHeaders(_ context.Context, req *http.Request) error {
	req.Header["Host"] = []string{req.Host}
	req.Header["User-Agent"] = []string{UserAgent}
	req.Header["Accept"] = []string{"*/*"}
	return nil
}

func NewHTTPClient(ctx context.Context, url string, config *config.Config) (Driver, error) {
	token, oCfg, ctxClient := getAuthToken(ctx, config)

	if !strings.Contains(url, ":") {
		url = fmt.Sprintf("%s:%d", url, DefaultHTTPPort)
	}

	if !strings.Contains(url, "://") {
		url = "https://" + url
	}

	hc := &http.Client{Transport: &http.Transport{TLSClientConfig: config.TLS}}
	if token.AccessToken != "" || token.RefreshToken != "" {
		hc = oCfg.Client(ctxClient, token)
	}

	c, err := apiHTTP.NewClientWithResponses(url, apiHTTP.WithHTTPClient(hc), apiHTTP.WithRequestEditorFn(setHeaders))

	return &driver{driverWithOptions: &httpDriver{httpCRUD: &httpCRUD{api: c}}}, err
}

func (c *httpDriver) Close() error {
	return nil
}

func (c *httpDriver) ListDatabases(ctx context.Context) ([]string, error) {
	resp, err := c.api.TigrisDBListDatabases(ctx)
	if err := HTTPError(err, resp); err != nil {
		return nil, err
	}
	var l apiHTTP.ListDatabasesResponse
	if err := respDecode(resp.Body, &l); err != nil {
		return nil, err
	}
	if l.Dbs == nil {
		return nil, nil
	}
	return *l.Dbs, nil
}

func (c *httpDriver) ListCollections(ctx context.Context, db string) ([]string, error) {
	resp, err := c.api.TigrisDBListCollections(ctx, db)
	if err := HTTPError(err, resp); err != nil {
		return nil, err
	}
	var l apiHTTP.ListCollectionsResponse
	if err := respDecode(resp.Body, &l); err != nil {
		return nil, err
	}
	if l.Collections == nil {
		return nil, nil
	}
	return *l.Collections, nil
}

func convertDatabaseOptions(_ *DatabaseOptions) *apiHTTP.DatabaseOptions {
	return &apiHTTP.DatabaseOptions{}
}

func convertCollectionOptions(_ *CollectionOptions) *apiHTTP.CollectionOptions {
	return &apiHTTP.CollectionOptions{}
}

func convertTransactionOptions(_ *TxOptions) *apiHTTP.TransactionOptions {
	return &apiHTTP.TransactionOptions{}
}

func (c *httpDriver) createDatabaseWithOptions(ctx context.Context, db string, options *DatabaseOptions) error {
	resp, err := c.api.TigrisDBCreateDatabase(ctx, db, apiHTTP.TigrisDBCreateDatabaseJSONRequestBody{
		Options: convertDatabaseOptions(options),
	})
	return HTTPError(err, resp)
}

func (c *httpDriver) dropDatabaseWithOptions(ctx context.Context, db string, options *DatabaseOptions) error {
	resp, err := c.api.TigrisDBDropDatabase(ctx, db, apiHTTP.TigrisDBDropDatabaseJSONRequestBody{
		Options: convertDatabaseOptions(options),
	})
	return HTTPError(err, resp)
}

func (c *httpDriver) createCollectionWithOptions(ctx context.Context, db string, collection string, schema Schema, options *CollectionOptions) error {
	resp, err := c.api.TigrisDBCreateCollection(ctx, db, collection, apiHTTP.TigrisDBCreateCollectionJSONRequestBody{
		Schema:  json.RawMessage(schema),
		Options: convertCollectionOptions(options),
	})
	return HTTPError(err, resp)
}

func (c *httpDriver) alterCollectionWithOptions(ctx context.Context, db string, collection string, schema Schema, options *CollectionOptions) error {
	resp, err := c.api.TigrisDBAlterCollection(ctx, db, collection, apiHTTP.TigrisDBAlterCollectionJSONRequestBody{
		Schema:  json.RawMessage(schema),
		Options: convertCollectionOptions(options),
	})
	return HTTPError(err, resp)
}

func (c *httpDriver) dropCollectionWithOptions(ctx context.Context, db string, collection string, _ *CollectionOptions) error {
	resp, err := c.api.TigrisDBDropCollection(ctx, db, collection)
	return HTTPError(err, resp)
}

func (c *httpDriver) beginTxWithOptions(ctx context.Context, db string, options *TxOptions) (txWithOptions, error) {
	resp, err := c.api.TigrisDBBeginTransaction(ctx, db, apiHTTP.TigrisDBBeginTransactionJSONRequestBody{
		Options: convertTransactionOptions(options),
	})
	if err := HTTPError(err, resp); err != nil {
		return nil, err
	}

	var bTx apiHTTP.BeginTransactionResponse
	if err = respDecode(resp.Body, &bTx); err != nil {
		return nil, err
	}

	if bTx.TxCtx == nil || bTx.TxCtx.Id == nil || *bTx.TxCtx.Id == "" {
		return nil, HTTPError(fmt.Errorf("empty transaction context in response"), nil)
	}

	return &httpTx{db: db, httpCRUD: &httpCRUD{api: c.api, txCtx: *bTx.TxCtx}}, nil
}

type httpTx struct {
	db string
	*httpCRUD
}

func (c *httpTx) Commit(ctx context.Context) error {
	resp, err := c.api.TigrisDBCommitTransaction(ctx, c.db, apiHTTP.TigrisDBCommitTransactionJSONRequestBody{
		TxCtx: &c.txCtx,
	})
	return HTTPError(err, resp)
}

func (c *httpTx) Rollback(ctx context.Context) error {
	resp, err := c.api.TigrisDBRollbackTransaction(ctx, c.db, apiHTTP.TigrisDBRollbackTransactionJSONRequestBody{
		TxCtx: &c.txCtx,
	})
	return HTTPError(err, resp)
}

func (c *httpTx) insertWithOptions(ctx context.Context, collection string, docs []Document, options *InsertOptions) (InsertResponse, error) {
	return c.httpCRUD.insertWithOptions(ctx, c.db, collection, docs, options)
}

func (c *httpTx) updateWithOptions(ctx context.Context, collection string, filter Filter, fields Fields, options *UpdateOptions) (UpdateResponse, error) {
	return c.httpCRUD.updateWithOptions(ctx, c.db, collection, filter, fields, options)
}

func (c *httpTx) deleteWithOptions(ctx context.Context, collection string, filter Filter, options *DeleteOptions) (DeleteResponse, error) {
	return c.httpCRUD.deleteWithOptions(ctx, c.db, collection, filter, options)
}

func (c *httpTx) readWithOptions(ctx context.Context, collection string, filter Filter, options *ReadOptions) (Iterator, error) {
	return c.httpCRUD.readWithOptions(ctx, c.db, collection, filter, options)
}

type httpCRUD struct {
	api   *apiHTTP.ClientWithResponses
	txCtx apiHTTP.TransactionCtx
}

func (c *httpCRUD) convertWriteOptions(_ *WriteOptions) *apiHTTP.WriteOptions {
	opts := apiHTTP.WriteOptions{}
	if c.txCtx.Id != nil && *c.txCtx.Id != "" {
		opts.TxCtx = &apiHTTP.TransactionCtx{Id: c.txCtx.Id, Origin: c.txCtx.Origin}
	}
	return &opts
}

func (c *httpCRUD) convertInsertOptions(_ *InsertOptions) *apiHTTP.InsertRequestOptions {
	return &apiHTTP.InsertRequestOptions{WriteOptions: c.convertWriteOptions(&WriteOptions{})}
}

func (c *httpCRUD) convertUpdateOptions(_ *UpdateOptions) *apiHTTP.UpdateRequestOptions {
	return &apiHTTP.UpdateRequestOptions{WriteOptions: c.convertWriteOptions(&WriteOptions{})}
}

func (c *httpCRUD) convertDeleteOptions(_ *DeleteOptions) *apiHTTP.DeleteRequestOptions {
	return &apiHTTP.DeleteRequestOptions{WriteOptions: c.convertWriteOptions(&WriteOptions{})}
}

func (c *httpCRUD) convertReadOptions(_ *ReadOptions) *apiHTTP.ReadRequestOptions {
	opts := apiHTTP.ReadRequestOptions{}
	if c.txCtx.Id != nil && *c.txCtx.Id != "" {
		opts.TxCtx = &apiHTTP.TransactionCtx{Id: c.txCtx.Id, Origin: c.txCtx.Origin}
	}
	return &opts
}

func (c *httpCRUD) insertWithOptions(ctx context.Context, db string, collection string, docs []Document, options *InsertOptions) (InsertResponse, error) {
	resp, err := c.api.TigrisDBInsert(ctx, db, collection, apiHTTP.TigrisDBInsertJSONRequestBody{
		Documents: (*[]json.RawMessage)(unsafe.Pointer(&docs)),
		Options:   c.convertInsertOptions(options),
	})
	return nil, HTTPError(err, resp)
}

func (c *httpCRUD) updateWithOptions(ctx context.Context, db string, collection string, filter Filter, fields Fields, options *UpdateOptions) (UpdateResponse, error) {
	resp, err := c.api.TigrisDBUpdate(ctx, db, collection, apiHTTP.TigrisDBUpdateJSONRequestBody{
		Filter:  json.RawMessage(filter),
		Fields:  json.RawMessage(fields),
		Options: c.convertUpdateOptions(options),
	})
	return nil, HTTPError(err, resp)
}

func (c *httpCRUD) deleteWithOptions(ctx context.Context, db string, collection string, filter Filter, options *DeleteOptions) (DeleteResponse, error) {
	resp, err := c.api.TigrisDBDelete(ctx, db, collection, apiHTTP.TigrisDBDeleteJSONRequestBody{
		Filter:  json.RawMessage(filter),
		Options: c.convertDeleteOptions(options),
	})
	return nil, HTTPError(err, resp)
}

func (c *httpCRUD) readWithOptions(ctx context.Context, db string, collection string, filter Filter, options *ReadOptions) (Iterator, error) {
	resp, err := c.api.TigrisDBRead(ctx, db, collection, apiHTTP.TigrisDBReadJSONRequestBody{
		Filter:  json.RawMessage(filter),
		Options: c.convertReadOptions(options),
	})

	if err != nil {
		return nil, HTTPError(err, nil)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return nil, HTTPError(fmt.Errorf(resp.Status), nil)
	}

	dec := json.NewDecoder(resp.Body)

	return &readIterator{streamReader: &httpStreamReader{stream: dec, closer: resp.Body}}, nil
}

type httpStreamReader struct {
	closer io.Closer
	stream *json.Decoder
}

func (g *httpStreamReader) read() (Document, error) {
	var res struct {
		Result apiHTTP.ReadResponse
	}
	if err := g.stream.Decode(&res); err != nil {
		return nil, HTTPError(err, nil)
	}

	return Document(res.Result.Doc), nil
}

func (g *httpStreamReader) close() error {
	return g.closer.Close()
}
