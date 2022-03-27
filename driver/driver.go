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
	"reflect"

	"github.com/tigrisdata/tigrisdb-client-go/config"
)

// Driver implements TigrisDB API
type Driver interface {
	// Insert or replace array of documents into specified database and collection
	Insert(ctx context.Context, db string, collection string, docs []Document, options ...*InsertOptions) (InsertResponse, error)
	// Read documents matching specified filter in the specified database and collection
	Read(ctx context.Context, db string, collection string, filter Filter, options ...*ReadOptions) (Iterator, error)
	// Update documents matching specified filter, with provided fields projection
	Update(ctx context.Context, db string, collection string, filter Filter, fields Fields, options ...*UpdateOptions) (UpdateResponse, error)
	// Delete documents matching specified filter form the specified database and collection
	Delete(ctx context.Context, db string, collection string, filter Filter, options ...*DeleteOptions) (DeleteResponse, error)
	// CreateCollection of documents in the database
	CreateCollection(ctx context.Context, db string, collection string, schema Schema, options ...*CollectionOptions) error
	// AlterCollection changes the schema of the existing collection
	AlterCollection(ctx context.Context, db string, collection string, schema Schema, options ...*CollectionOptions) error
	// DropCollection deletes the collection and all documents it contains
	DropCollection(ctx context.Context, db string, collection string, options ...*CollectionOptions) error
	// CreateDatabase creates new database
	CreateDatabase(ctx context.Context, db string, options ...*DatabaseOptions) error
	// DropDatabase deletes the database and all collections it contains
	DropDatabase(ctx context.Context, db string, options ...*DatabaseOptions) error
	// ListCollections lists collectdions in the database
	ListCollections(ctx context.Context, db string) ([]string, error)
	// ListDatabases in the current namespace
	ListDatabases(ctx context.Context) ([]string, error)
	// BeginTx starts new transaction
	BeginTx(ctx context.Context, db string, options ...*TxOptions) (Tx, error)
	// Close releases resources of the driver
	Close() error
}

// Tx object is used to atomically modify documents.
// This object is returned by BeginTx
type Tx interface {
	// Insert or replace document into specified collection
	Insert(ctx context.Context, collection string, docs []Document, options ...*InsertOptions) (InsertResponse, error)
	// Read documents from the collection matching the specified filter
	Read(ctx context.Context, collection string, filter Filter, options ...*ReadOptions) (Iterator, error)
	// Update documents in the collection matching the speficied filter
	Update(ctx context.Context, collection string, filter Filter, fields Fields, options ...*UpdateOptions) (UpdateResponse, error)
	// Delete documents from the collection matching specified filter
	Delete(ctx context.Context, collection string, filter Filter, options ...*DeleteOptions) (DeleteResponse, error)
	// Commit all the modification of the transaction
	Commit(ctx context.Context) error
	// Rollback discard all the modification made by the transaction
	Rollback(ctx context.Context) error
}

type driver struct {
	driverWithOptions
}

func (c *driver) Insert(ctx context.Context, db string, collection string, docs []Document, options ...*InsertOptions) (InsertResponse, error) {
	opts, err := validateOptionsParam(options)
	if err != nil {
		return nil, err
	}

	return c.insertWithOptions(ctx, db, collection, docs, opts.(*InsertOptions))
}

func (c *driver) Update(ctx context.Context, db string, collection string, filter Filter, fields Fields, options ...*UpdateOptions) (UpdateResponse, error) {
	opts, err := validateOptionsParam(options)
	if err != nil {
		return nil, err
	}

	return c.updateWithOptions(ctx, db, collection, filter, fields, opts.(*UpdateOptions))
}

func (c *driver) Delete(ctx context.Context, db string, collection string, filter Filter, options ...*DeleteOptions) (DeleteResponse, error) {
	opts, err := validateOptionsParam(options)
	if err != nil {
		return nil, err
	}

	return c.deleteWithOptions(ctx, db, collection, filter, opts.(*DeleteOptions))
}

func (c *driver) Read(ctx context.Context, db string, collection string, filter Filter, options ...*ReadOptions) (Iterator, error) {
	opts, err := validateOptionsParam(options)
	if err != nil {
		return nil, err
	}

	return c.readWithOptions(ctx, db, collection, filter, opts.(*ReadOptions))
}

func (c *driver) CreateCollection(ctx context.Context, db string, collection string, schema Schema, options ...*CollectionOptions) error {
	opts, err := validateOptionsParam(options)
	if err != nil {
		return err
	}

	return c.createCollectionWithOptions(ctx, db, collection, schema, opts.(*CollectionOptions))
}

func (c *driver) AlterCollection(ctx context.Context, db string, collection string, schema Schema, options ...*CollectionOptions) error {
	opts, err := validateOptionsParam(options)
	if err != nil {
		return err
	}

	return c.alterCollectionWithOptions(ctx, db, collection, schema, opts.(*CollectionOptions))
}

func (c *driver) DropCollection(ctx context.Context, db string, collection string, options ...*CollectionOptions) error {
	opts, err := validateOptionsParam(options)
	if err != nil {
		return err
	}

	return c.dropCollectionWithOptions(ctx, db, collection, opts.(*CollectionOptions))
}

func (c *driver) CreateDatabase(ctx context.Context, db string, options ...*DatabaseOptions) error {
	opts, err := validateOptionsParam(options)
	if err != nil {
		return err
	}

	return c.createDatabaseWithOptions(ctx, db, opts.(*DatabaseOptions))
}

func (c *driver) DropDatabase(ctx context.Context, db string, options ...*DatabaseOptions) error {
	opts, err := validateOptionsParam(options)
	if err != nil {
		return err
	}

	return c.dropDatabaseWithOptions(ctx, db, opts.(*DatabaseOptions))
}

func (c *driver) BeginTx(ctx context.Context, db string, options ...*TxOptions) (Tx, error) {
	opts, err := validateOptionsParam(options)
	if err != nil {
		return nil, err
	}

	tx, err := c.beginTxWithOptions(ctx, db, opts.(*TxOptions))
	if err != nil {
		return nil, err
	}
	return &driverTxWithOptions{txWithOptions: tx, db: db}, nil
}

type driverTxWithOptions struct {
	txWithOptions
	db string
}

func (c *driverTxWithOptions) Insert(ctx context.Context, collection string, docs []Document, options ...*InsertOptions) (InsertResponse, error) {
	opts, err := validateOptionsParam(options)
	if err != nil {
		return nil, err
	}

	return c.insertWithOptions(ctx, collection, docs, opts.(*InsertOptions))
}

func (c *driverTxWithOptions) Update(ctx context.Context, collection string, filter Filter, fields Fields, options ...*UpdateOptions) (UpdateResponse, error) {
	opts, err := validateOptionsParam(options)
	if err != nil {
		return nil, err
	}

	return c.updateWithOptions(ctx, collection, filter, fields, opts.(*UpdateOptions))
}

func (c *driverTxWithOptions) Delete(ctx context.Context, collection string, filter Filter, options ...*DeleteOptions) (DeleteResponse, error) {
	opts, err := validateOptionsParam(options)
	if err != nil {
		return nil, err
	}

	return c.deleteWithOptions(ctx, collection, filter, opts.(*DeleteOptions))
}

func (c *driverTxWithOptions) Read(ctx context.Context, collection string, filter Filter, options ...*ReadOptions) (Iterator, error) {
	opts, err := validateOptionsParam(options)
	if err != nil {
		return nil, err
	}

	return c.readWithOptions(ctx, collection, filter, opts.(*ReadOptions))
}

func validateOptionsParam(options interface{}) (interface{}, error) {
	v := reflect.ValueOf(options)

	if (v.Kind() != reflect.Array && v.Kind() != reflect.Slice) || v.Len() > 1 {
		return nil, fmt.Errorf("API accepts no more then one options parameter")
	}

	if v.Len() < 1 {
		return nil, nil
	}

	return v.Index(0).Interface(), nil
}

// NewDriver connect to TigrisDB at the specified URL
// URL should be in the form: {hostname}:{port}
func NewDriver(ctx context.Context, cfg *config.Config) (Driver, error) {
	if cfg == nil {
		cfg = &config.Config{}
	}
	if DefaultProtocol == GRPC {
		return NewGRPCClient(ctx, cfg.URL, cfg)
	} else if DefaultProtocol == HTTP {
		return NewHTTPClient(ctx, cfg.URL, cfg)
	}
	return nil, fmt.Errorf("unsupported protocol")
}
