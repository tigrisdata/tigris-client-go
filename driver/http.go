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
	"time"
	"unsafe"

	"google.golang.org/protobuf/types/known/timestamppb"

	apiHTTP "github.com/tigrisdata/tigris-client-go/api/client/v1/api"
	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
	"github.com/tigrisdata/tigris-client-go/config"
)

const (
	DefaultHTTPPort = 443
)

type txCtxKey struct{}

// HTTPError parses HTTP error into TigrisError
// Returns nil, if HTTP status is OK
func HTTPError(err error, resp *http.Response) error {
	if err != nil {
		if terr, ok := err.(*api.TigrisError); ok {
			return &Error{TigrisError: terr}
		}

		if err == io.EOF {
			return err
		}

		return err
	}

	if resp == nil {
		return nil
	}

	if resp.StatusCode == http.StatusOK {
		return nil
	}

	defer func() {
		if resp != nil {
			_ = resp.Body.Close()
		}
	}()

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if !strings.Contains(resp.Header.Get("Content-Type"), "json") {
		return &Error{&api.TigrisError{Code: api.Code_UNKNOWN, Message: string(b)}}
	}

	te := api.UnmarshalStatus(b)

	return &Error{TigrisError: te}
}

type httpDriver struct {
	api *apiHTTP.ClientWithResponses
}

func respDecode(body io.ReadCloser, v interface{}) error {
	defer func() {
		_ = body.Close()
	}()

	if err := json.NewDecoder(body).Decode(v); err != nil {
		return err
	}
	return nil
}

type metadata struct {
	CreatedAt *time.Time `json:"created_at,omitempty"`
	UpdatedAt *time.Time `json:"updated_at,omitempty"`
	DeletedAt *time.Time `json:"deleted_at,omitempty"`
}

func newRespMetadata(m *metadata) *api.ResponseMetadata {
	r := &api.ResponseMetadata{}
	if m.CreatedAt != nil {
		r.CreatedAt = timestamppb.New(*m.CreatedAt)
	}
	if m.UpdatedAt != nil {
		r.UpdatedAt = timestamppb.New(*m.UpdatedAt)
	}
	if m.DeletedAt != nil {
		r.DeletedAt = timestamppb.New(*m.DeletedAt)
	}
	return r
}

// convert timestamps from response metadata to timestamppb.Timestamp
func dmlRespDecode(body io.ReadCloser, v interface{}) error {
	r := struct {
		Metadata      metadata
		Status        string `json:"status,omitempty"`
		ModifiedCount int32  `json:"modified_count,omitempty"`
	}{}

	if err := respDecode(body, &r); err != nil {
		return err
	}

	switch v := v.(type) {
	case *InsertResponse:
		v.Status = r.Status
		v.Metadata = newRespMetadata(&r.Metadata)
	case *ReplaceResponse:
		v.Status = r.Status
		v.Metadata = newRespMetadata(&r.Metadata)
	case *UpdateResponse:
		v.Status = r.Status
		v.ModifiedCount = r.ModifiedCount
		v.Metadata = newRespMetadata(&r.Metadata)
	case *DeleteResponse:
		v.Status = r.Status
		v.Metadata = newRespMetadata(&r.Metadata)
	default:
		return fmt.Errorf("unkknown response type")
	}

	return nil
}

func setHeaders(ctx context.Context, req *http.Request) error {
	req.Header["Host"] = []string{req.Host}
	req.Header["User-Agent"] = []string{UserAgent}
	req.Header["Accept"] = []string{"*/*"}

	if v := ctx.Value(txCtxKey{}); v != nil {
		txCtx := v.(*api.TransactionCtx)
		req.Header[api.HeaderTxID] = []string{txCtx.Id}
		req.Header[api.HeaderTxOrigin] = []string{txCtx.Origin}
	}

	return nil
}

func setHTTPTxCtx(ctx context.Context, txCtx *api.TransactionCtx) context.Context {
	if txCtx != nil && txCtx.Id != "" {
		return context.WithValue(ctx, txCtxKey{}, txCtx)
	}
	return ctx
}

// NewHTTPClient return Driver interface implementation using HTTP transport protocol
func NewHTTPClient(ctx context.Context, url string, config *config.Driver) (Driver, error) {
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
	if err != nil {
		return nil, err
	}

	//	return &driver{driverWithOptions: &httpDriver{httpCRUD: &httpCRUD{api: c}}}, err
	return &driver{driverWithOptions: &httpDriver{api: c}}, nil
}

func (c *httpDriver) Close() error {
	return nil
}

func (c *httpDriver) UseDatabase(name string) Database {
	return &driverCRUD{&httpCRUD{db: name, api: c.api}}
}

func (c *httpDriver) Info(ctx context.Context) (*InfoResponse, error) {
	resp, err := c.api.TigrisGetInfo(ctx)
	if err := HTTPError(err, resp); err != nil {
		return nil, err
	}
	var i InfoResponse
	if err := respDecode(resp.Body, &i); err != nil {
		return nil, err
	}

	return &i, nil
}

func (c *httpDriver) ListDatabases(ctx context.Context) ([]string, error) {
	resp, err := c.api.TigrisListDatabases(ctx)
	if err := HTTPError(err, resp); err != nil {
		return nil, err
	}
	var l api.ListDatabasesResponse
	if err := respDecode(resp.Body, &l); err != nil {
		return nil, err
	}
	if l.Databases == nil {
		return nil, nil
	}

	var databases []string
	for _, nm := range l.Databases {
		databases = append(databases, nm.Db)
	}
	return databases, nil
}

func (c *httpDriver) DescribeDatabase(ctx context.Context, db string) (*DescribeDatabaseResponse, error) {
	resp, err := c.api.TigrisDescribeDatabase(ctx, db, apiHTTP.TigrisDescribeDatabaseJSONRequestBody{})
	if err := HTTPError(err, resp); err != nil {
		return nil, err
	}
	var d apiHTTP.DescribeDatabaseResponse
	if err := respDecode(resp.Body, &d); err != nil {
		return nil, err
	}

	var r DescribeDatabaseResponse
	r.Db = ToString(d.Db)
	for _, v := range *d.Collections {
		r.Collections = append(r.Collections, &api.CollectionDescription{
			Collection: ToString(v.Collection),
			Schema:     v.Schema,
		})
	}
	return &r, nil
}

func convertDatabaseOptions(_ *DatabaseOptions) *apiHTTP.DatabaseOptions {
	return &apiHTTP.DatabaseOptions{}
}

func (c *httpCRUD) convertCollectionOptions(_ *CollectionOptions) *apiHTTP.CollectionOptions {
	opts := apiHTTP.CollectionOptions{}
	return &opts
}

func convertTransactionOptions(_ *TxOptions) *apiHTTP.TransactionOptions {
	return &apiHTTP.TransactionOptions{}
}

func (c *httpDriver) createDatabaseWithOptions(ctx context.Context, db string, options *DatabaseOptions) error {
	resp, err := c.api.TigrisCreateDatabase(ctx, db, apiHTTP.TigrisCreateDatabaseJSONRequestBody{
		Options: convertDatabaseOptions(options),
	})
	return HTTPError(err, resp)
}

func (c *httpDriver) dropDatabaseWithOptions(ctx context.Context, db string, options *DatabaseOptions) error {
	resp, err := c.api.TigrisDropDatabase(ctx, db, apiHTTP.TigrisDropDatabaseJSONRequestBody{
		Options: convertDatabaseOptions(options),
	})
	return HTTPError(err, resp)
}

func (c *httpDriver) beginTxWithOptions(ctx context.Context, db string, options *TxOptions) (txWithOptions, error) {
	resp, err := c.api.TigrisBeginTransaction(ctx, db, apiHTTP.TigrisBeginTransactionJSONRequestBody{
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

	return &httpCRUD{db: db, api: c.api, txCtx: &api.TransactionCtx{Id: ToString(bTx.TxCtx.Id), Origin: ToString(bTx.TxCtx.Origin)}}, nil
}

func (c *httpCRUD) Commit(ctx context.Context) error {
	ctx = setHTTPTxCtx(ctx, c.txCtx)

	resp, err := c.api.TigrisCommitTransaction(ctx, c.db, apiHTTP.TigrisCommitTransactionJSONRequestBody{})
	err = HTTPError(err, resp)
	if err == nil {
		c.committed = true
	}
	return err
}

func (c *httpCRUD) Rollback(ctx context.Context) error {
	ctx = setHTTPTxCtx(ctx, c.txCtx)

	if c.committed {
		return nil
	}
	resp, err := c.api.TigrisRollbackTransaction(ctx, c.db, apiHTTP.TigrisRollbackTransactionJSONRequestBody{})
	return HTTPError(err, resp)
}

type httpCRUD struct {
	db    string
	api   *apiHTTP.ClientWithResponses
	txCtx *api.TransactionCtx

	committed bool
}

func (c *httpCRUD) convertWriteOptions(o *api.WriteOptions) *apiHTTP.WriteOptions {
	if o != nil {
		return &apiHTTP.WriteOptions{}
	}
	return nil
}

func (c *httpCRUD) convertInsertOptions(o *InsertOptions) *apiHTTP.InsertRequestOptions {
	return &apiHTTP.InsertRequestOptions{WriteOptions: c.convertWriteOptions(o.WriteOptions)}
}

func (c *httpCRUD) convertReplaceOptions(o *ReplaceOptions) *apiHTTP.ReplaceRequestOptions {
	return &apiHTTP.ReplaceRequestOptions{WriteOptions: c.convertWriteOptions(o.WriteOptions)}
}

func (c *httpCRUD) convertUpdateOptions(o *UpdateOptions) *apiHTTP.UpdateRequestOptions {
	return &apiHTTP.UpdateRequestOptions{WriteOptions: c.convertWriteOptions(o.WriteOptions)}
}

func (c *httpCRUD) convertDeleteOptions(o *DeleteOptions) *apiHTTP.DeleteRequestOptions {
	return &apiHTTP.DeleteRequestOptions{WriteOptions: c.convertWriteOptions(o.WriteOptions)}
}

func (c *httpCRUD) convertReadOptions(_ *ReadOptions) *apiHTTP.ReadRequestOptions {
	opts := apiHTTP.ReadRequestOptions{}
	return &opts
}

func (c *httpCRUD) convertEventsOptions(_ *EventsOptions) *apiHTTP.EventsRequestOptions {
	return &apiHTTP.EventsRequestOptions{}
}

func (c *httpCRUD) convertPublishOptions(o *PublishOptions) *apiHTTP.PublishRequestOptions {
	return &apiHTTP.PublishRequestOptions{}
}

func (c *httpCRUD) convertSubscribeOptions(_ *SubscribeOptions) *apiHTTP.SubscribeRequestOptions {
	return &apiHTTP.SubscribeRequestOptions{}
}

func (c *httpCRUD) listCollectionsWithOptions(ctx context.Context, options *CollectionOptions) ([]string, error) {
	ctx = setHTTPTxCtx(ctx, c.txCtx)

	resp, err := c.api.TigrisListCollections(ctx, c.db, apiHTTP.TigrisListCollectionsJSONRequestBody{
		Options: c.convertCollectionOptions(options),
	})
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

	var collections []string
	for _, c := range *l.Collections {
		if c.Collection != nil {
			collections = append(collections, *c.Collection)
		}
	}
	return collections, nil
}

func (c *httpCRUD) describeCollectionWithOptions(ctx context.Context, collection string, _ *CollectionOptions) (*DescribeCollectionResponse, error) {
	resp, err := c.api.TigrisDescribeCollection(ctx, c.db, collection, apiHTTP.TigrisDescribeCollectionJSONRequestBody{})
	if err := HTTPError(err, resp); err != nil {
		return nil, err
	}
	var d apiHTTP.DescribeCollectionResponse
	if err := respDecode(resp.Body, &d); err != nil {
		return nil, err
	}

	r := &DescribeCollectionResponse{
		Schema:     d.Schema,
		Collection: ToString(d.Collection),
	}

	return r, nil
}

func (c *httpCRUD) createOrUpdateCollectionWithOptions(ctx context.Context, collection string, schema Schema, options *CollectionOptions) error {
	ctx = setHTTPTxCtx(ctx, c.txCtx)

	resp, err := c.api.TigrisCreateOrUpdateCollection(ctx, c.db, collection, apiHTTP.TigrisCreateOrUpdateCollectionJSONRequestBody{
		Schema:  json.RawMessage(schema),
		Options: c.convertCollectionOptions(options),
	})
	return HTTPError(err, resp)
}

func (c *httpCRUD) dropCollectionWithOptions(ctx context.Context, collection string, options *CollectionOptions) error {
	ctx = setHTTPTxCtx(ctx, c.txCtx)

	resp, err := c.api.TigrisDropCollection(ctx, c.db, collection, apiHTTP.TigrisDropCollectionJSONRequestBody{
		Options: c.convertCollectionOptions(options),
	})
	return HTTPError(err, resp)
}

func (c *httpCRUD) insertWithOptions(ctx context.Context, collection string, docs []Document, options *InsertOptions) (*InsertResponse, error) {
	ctx = setHTTPTxCtx(ctx, c.txCtx)

	resp, err := c.api.TigrisInsert(ctx, c.db, collection, apiHTTP.TigrisInsertJSONRequestBody{
		Documents: (*[]json.RawMessage)(unsafe.Pointer(&docs)),
		Options:   c.convertInsertOptions(options),
	})

	if err = HTTPError(err, resp); err != nil {
		return nil, err
	}

	var d InsertResponse
	if err := dmlRespDecode(resp.Body, &d); err != nil {
		return nil, err
	}

	return &d, nil
}

func (c *httpCRUD) replaceWithOptions(ctx context.Context, collection string, docs []Document, options *ReplaceOptions) (*ReplaceResponse, error) {
	ctx = setHTTPTxCtx(ctx, c.txCtx)

	resp, err := c.api.TigrisReplace(ctx, c.db, collection, apiHTTP.TigrisReplaceJSONRequestBody{
		Documents: (*[]json.RawMessage)(unsafe.Pointer(&docs)),
		Options:   c.convertReplaceOptions(options),
	})

	if err = HTTPError(err, resp); err != nil {
		return nil, err
	}

	var d ReplaceResponse
	if err := dmlRespDecode(resp.Body, &d); err != nil {
		return nil, err
	}

	return &d, nil
}

func (c *httpCRUD) updateWithOptions(ctx context.Context, collection string, filter Filter, fields Update, options *UpdateOptions) (*UpdateResponse, error) {
	ctx = setHTTPTxCtx(ctx, c.txCtx)

	resp, err := c.api.TigrisUpdate(ctx, c.db, collection, apiHTTP.TigrisUpdateJSONRequestBody{
		Filter:  json.RawMessage(filter),
		Fields:  json.RawMessage(fields),
		Options: c.convertUpdateOptions(options),
	})

	if err = HTTPError(err, resp); err != nil {
		return nil, err
	}

	var d UpdateResponse
	if err := dmlRespDecode(resp.Body, &d); err != nil {
		return nil, err
	}

	return &d, nil
}

func (c *httpCRUD) deleteWithOptions(ctx context.Context, collection string, filter Filter, options *DeleteOptions) (*DeleteResponse, error) {
	ctx = setHTTPTxCtx(ctx, c.txCtx)

	resp, err := c.api.TigrisDelete(ctx, c.db, collection, apiHTTP.TigrisDeleteJSONRequestBody{
		Filter:  json.RawMessage(filter),
		Options: c.convertDeleteOptions(options),
	})

	if err = HTTPError(err, resp); err != nil {
		return nil, err
	}

	var d DeleteResponse
	if err := dmlRespDecode(resp.Body, &d); err != nil {
		return nil, err
	}

	return &d, nil
}

func (c *httpCRUD) readWithOptions(ctx context.Context, collection string, filter Filter, fields Projection, options *ReadOptions) (Iterator, error) {
	ctx = setHTTPTxCtx(ctx, c.txCtx)

	resp, err := c.api.TigrisRead(ctx, c.db, collection, apiHTTP.TigrisReadJSONRequestBody{
		Filter:  json.RawMessage(filter),
		Fields:  json.RawMessage(fields),
		Options: c.convertReadOptions(options),
	})

	err = HTTPError(err, resp)
	dec := json.NewDecoder(resp.Body)

	return &readIterator{streamReader: &httpStreamReader{stream: dec, closer: resp.Body}, err: err, eof: err != nil}, nil
}

type httpStreamReader struct {
	closer io.Closer
	stream *json.Decoder
}

func (g *httpStreamReader) read() (Document, error) {
	var res struct {
		Result *apiHTTP.ReadResponse
		Error  *api.ErrorDetails
	}

	if err := g.stream.Decode(&res); err != nil {
		return nil, HTTPError(err, nil)
	}

	if res.Error != nil {
		return nil, &Error{TigrisError: api.FromErrorDetails(res.Error)}
	}

	return Document(res.Result.Data), nil
}

func (g *httpStreamReader) close() error {
	return g.closer.Close()
}

func (c *httpCRUD) search(ctx context.Context, collection string, req *SearchRequest) (SearchResultIterator, error) {
	// Will anything break if we don't set transaction context here?
	if req.SearchFields == nil {
		req.SearchFields = []string{}
	}
	resp, err := c.api.TigrisSearch(ctx, c.db, collection, apiHTTP.TigrisSearchJSONRequestBody{
		Q:            &req.Q,
		SearchFields: &req.SearchFields,
		Filter:       json.RawMessage(req.Filter),
		Facet:        json.RawMessage(req.Facet),
		Fields:       json.RawMessage(req.ReadFields),
		Page:         &req.Page,
		PageSize:     &req.PageSize,
	})
	err = HTTPError(err, resp)
	dec := json.NewDecoder(resp.Body)

	return &searchResultIterator{
		searchStreamReader: &httpSearchReader{
			closer: resp.Body,
			stream: dec,
		},
		eof: err != nil,
		err: err,
	}, nil
}

type httpSearchReader struct {
	closer io.Closer
	stream *json.Decoder
}

func (g *httpSearchReader) read() (SearchResponse, error) {
	var res struct {
		Result struct {
			Hits   []*api.SearchHit
			Facets map[string]*api.SearchFacet
			Meta   *api.SearchMetadata
		}
		Error *api.ErrorDetails
	}
	if err := g.stream.Decode(&res); err != nil {
		return nil, HTTPError(err, nil)
	}

	if res.Error != nil {
		return nil, &Error{TigrisError: api.FromErrorDetails(res.Error)}
	}

	return &api.SearchResponse{
		Hits:   res.Result.Hits,
		Facets: res.Result.Facets,
		Meta:   res.Result.Meta,
	}, nil
}

func (g *httpSearchReader) close() error {
	return g.closer.Close()
}

func (c *httpCRUD) eventsWithOptions(ctx context.Context, collection string, options *EventsOptions) (EventIterator, error) {
	resp, err := c.api.TigrisEvents(ctx, c.db, collection, apiHTTP.TigrisEventsJSONRequestBody{
		Collection: &collection,
		Options:    c.convertEventsOptions(options),
	})

	err = HTTPError(err, resp)
	dec := json.NewDecoder(resp.Body)

	return &eventReadIterator{eventStreamReader: &httpEventStreamReader{stream: dec, closer: resp.Body}, err: err, eof: err != nil}, nil
}

type httpEventStreamReader struct {
	closer io.Closer
	stream *json.Decoder
}

func (g *httpEventStreamReader) read() (Event, error) {
	var res struct {
		Result struct {
			Event Event
		}
		Error *api.ErrorDetails
	}

	if err := g.stream.Decode(&res); err != nil {
		return nil, HTTPError(err, nil)
	}

	if res.Error != nil {
		return nil, &Error{TigrisError: api.FromErrorDetails(res.Error)}
	}

	return res.Result.Event, nil
}

func (g *httpEventStreamReader) close() error {
	return g.closer.Close()
}

func ToString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func (c *httpCRUD) publishWithOptions(ctx context.Context, collection string, docs []Document, options *PublishOptions) (*PublishResponse, error) {
	ctx = setHTTPTxCtx(ctx, c.txCtx)

	resp, err := c.api.TigrisPublish(ctx, c.db, collection, apiHTTP.TigrisPublishJSONRequestBody{
		Messages: (*[]json.RawMessage)(unsafe.Pointer(&docs)),
		Options:  c.convertPublishOptions(options),
	})

	if err = HTTPError(err, resp); err != nil {
		return nil, err
	}

	var d PublishResponse
	if err := dmlRespDecode(resp.Body, &d); err != nil {
		return nil, err
	}

	return &d, nil
}

func (c *httpCRUD) subscribeWithOptions(ctx context.Context, collection string, options *SubscribeOptions) (Iterator, error) {
	ctx = setHTTPTxCtx(ctx, c.txCtx)

	resp, err := c.api.TigrisSubscribe(ctx, c.db, collection, apiHTTP.TigrisSubscribeJSONBody{
		Options: c.convertSubscribeOptions(options),
	})

	err = HTTPError(err, resp)
	dec := json.NewDecoder(resp.Body)

	return &readIterator{streamReader: &httpStreamReader{stream: dec, closer: resp.Body}, err: err, eof: err != nil}, nil
}
