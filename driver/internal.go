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
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/tigrisdata/tigrisdb-client-go/config"
	"golang.org/x/oauth2"
)

type driverWithOptions interface {
	insertWithOptions(ctx context.Context, db string, collection string, docs []Document, options *InsertOptions) (InsertResponse, error)
	readWithOptions(ctx context.Context, db string, collection string, filter Filter, options *ReadOptions) (Iterator, error)
	updateWithOptions(ctx context.Context, db string, collection string, filter Filter, fields Fields, options *UpdateOptions) (UpdateResponse, error)
	deleteWithOptions(ctx context.Context, db string, collection string, filter Filter, options *DeleteOptions) (DeleteResponse, error)
	createCollectionWithOptions(ctx context.Context, db string, collection string, schema Schema, options *CollectionOptions) error
	alterCollectionWithOptions(ctx context.Context, db string, collection string, schema Schema, options *CollectionOptions) error
	dropCollectionWithOptions(ctx context.Context, db string, collection string, options *CollectionOptions) error
	createDatabaseWithOptions(ctx context.Context, db string, options *DatabaseOptions) error
	dropDatabaseWithOptions(ctx context.Context, db string, options *DatabaseOptions) error
	beginTxWithOptions(ctx context.Context, db string, options *TxOptions) (txWithOptions, error)

	ListCollections(ctx context.Context, db string) ([]string, error)
	ListDatabases(ctx context.Context) ([]string, error)
	Close() error
}

type txWithOptions interface {
	insertWithOptions(ctx context.Context, collection string, docs []Document, options *InsertOptions) (InsertResponse, error)
	readWithOptions(ctx context.Context, collection string, filter Filter, options *ReadOptions) (Iterator, error)
	updateWithOptions(ctx context.Context, collection string, filter Filter, fields Fields, options *UpdateOptions) (UpdateResponse, error)
	deleteWithOptions(ctx context.Context, collection string, filter Filter, options *DeleteOptions) (DeleteResponse, error)
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

func getAuthToken(ctx context.Context, config *config.Config) (*oauth2.Token, *oauth2.Config, context.Context) {
	token := config.Token
	if os.Getenv(TokenEnv) != "" {
		token = os.Getenv(TokenEnv)
	}

	parts := strings.Split(token, ":")

	t := oauth2.Token{}

	if len(parts) > 0 {
		t.AccessToken = parts[0]
	}

	if len(parts) > 1 {
		t.RefreshToken = parts[1]
		// So as we have refresh token, just disregard current access token and refresh immediately
		t.Expiry = time.Now()
	}

	tr := &http.Transport{
		TLSClientConfig: config.TLS,
	}

	ocfg := &oauth2.Config{Endpoint: oauth2.Endpoint{TokenURL: ToekenRefreshURL}}

	return &t, ocfg, context.WithValue(ctx, oauth2.HTTPClient, &http.Client{Transport: tr})
}
