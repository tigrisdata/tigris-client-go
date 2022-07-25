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

// Package driver provides access to the low level Tigris API.
// It abstracts underlying transport protocol.
package driver

import (
	"context"
	"fmt"
	"reflect"

	"github.com/tigrisdata/tigris-client-go/config"
)

// Driver implements Tigris API
type Driver interface {
	// Info returns server information
	Info(ctx context.Context) (*InfoResponse, error)

	// UseDatabase returns and interface for collections and documents management
	// of the database
	UseDatabase(name string) Database

	// ListDatabases in the current namespace
	ListDatabases(ctx context.Context) ([]string, error)
	// DescribeDatabase returns database metadata
	DescribeDatabase(ctx context.Context, db string) (*DescribeDatabaseResponse, error)

	// CreateDatabase creates new database
	CreateDatabase(ctx context.Context, db string, options ...*DatabaseOptions) error
	// DropDatabase deletes the database and all collections it contains
	DropDatabase(ctx context.Context, db string, options ...*DatabaseOptions) error

	// BeginTx starts new transaction
	BeginTx(ctx context.Context, db string, options ...*TxOptions) (Tx, error)

	// Close releases resources of the driver
	Close() error
}

// Tx object is used to atomically modify documents.
// This object is returned by BeginTx
type Tx interface {
	// Commit all the modification of the transaction
	Commit(ctx context.Context) error

	// Rollback discard all the modification made by the transaction
	Rollback(ctx context.Context) error

	Database
}

// Database is the interface that encapsulates the CRUD portions of the transaction API.
type Database interface {
	// Insert array of documents into specified database and collection.
	Insert(ctx context.Context, collection string, docs []Document, options ...*InsertOptions) (*InsertResponse, error)

	// Replace array of documents into specified database and collection
	// Creates document if it doesn't exist.
	Replace(ctx context.Context, collection string, docs []Document, options ...*ReplaceOptions) (*ReplaceResponse, error)

	// Read documents from the collection matching the specified filter.
	Read(ctx context.Context, collection string, filter Filter, fields Projection, options ...*ReadOptions) (Iterator, error)

	//Search for documents in the collection matching specified query
	Search(ctx context.Context, collection string, request *SearchRequest) (SearchResultIterator, error)

	// Update documents in the collection matching the specified filter.
	Update(ctx context.Context, collection string, filter Filter, fields Update, options ...*UpdateOptions) (*UpdateResponse, error)

	// Delete documents from the collection matching specified filter.
	Delete(ctx context.Context, collection string, filter Filter, options ...*DeleteOptions) (*DeleteResponse, error)

	// CreateOrUpdateCollection either creates a collection or update the collection with the new schema
	// There are three categories of data types supported:
	//   Primitive: Strings, Numbers, Binary Data, Booleans, UUIDs, DateTime
	//   Complex: Arrays
	//   Objects: A container data type defined by the user that stores fields of primitive types,
	//   complex types as well as other Objects
	//
	//  The data types are derived from the types defined in the JSON schema specification
	//  with extensions that enable support for richer semantics.
	//  As an example, the string is defined like this,
	//   {
	//     "name": {
	//       "type": "string"
	//     }
	//   }
	// More detailed information here: https://docs.tigrisdata.com/datamodels/types
	CreateOrUpdateCollection(ctx context.Context, collection string, schema Schema, options ...*CollectionOptions) error

	// DropCollection deletes the collection and all documents it contains.
	DropCollection(ctx context.Context, collection string, options ...*CollectionOptions) error

	// ListCollections lists collections in the database.
	ListCollections(ctx context.Context, options ...*CollectionOptions) ([]string, error)

	// DescribeCollection returns metadata of the collection in the database
	DescribeCollection(ctx context.Context, collection string, options ...*CollectionOptions) (*DescribeCollectionResponse, error)

	Events(ctx context.Context, collection string, options ...*EventsOptions) (EventIterator, error)

	Publish(ctx context.Context, collection string, docs []Document, options ...*PublishOptions) (*PublishResponse, error)

	Subscribe(ctx context.Context, collection string, options ...*SubscribeOptions) (Iterator, error)
}

type driver struct {
	driverWithOptions
}

func (c *driver) CreateDatabase(ctx context.Context, db string, options ...*DatabaseOptions) error {
	opts, err := validateOptionsParam(options, &DatabaseOptions{})
	if err != nil {
		return err
	}

	return c.createDatabaseWithOptions(ctx, db, opts.(*DatabaseOptions))
}

func (c *driver) DropDatabase(ctx context.Context, db string, options ...*DatabaseOptions) error {
	opts, err := validateOptionsParam(options, &DatabaseOptions{})
	if err != nil {
		return err
	}

	return c.dropDatabaseWithOptions(ctx, db, opts.(*DatabaseOptions))
}
func (c *driver) BeginTx(ctx context.Context, db string, options ...*TxOptions) (Tx, error) {
	opts, err := validateOptionsParam(options, &TxOptions{})
	if err != nil {
		return nil, err
	}

	tx, err := c.beginTxWithOptions(ctx, db, opts.(*TxOptions))
	if err != nil {
		return nil, err
	}
	return &driverCRUDTx{driverCRUD: &driverCRUD{tx}, txWithOptions: tx}, nil
}

type driverCRUDTx struct {
	*driverCRUD
	txWithOptions
}

type driverCRUD struct {
	CRUDWithOptions
}

func (c *driverCRUD) Insert(ctx context.Context, collection string, docs []Document, options ...*InsertOptions) (*InsertResponse, error) {
	opts, err := validateOptionsParam(options, &InsertOptions{})
	if err != nil {
		return nil, err
	}

	return c.insertWithOptions(ctx, collection, docs, opts.(*InsertOptions))
}

func (c *driverCRUD) Replace(ctx context.Context, collection string, docs []Document, options ...*ReplaceOptions) (*ReplaceResponse, error) {
	opts, err := validateOptionsParam(options, &ReplaceOptions{})
	if err != nil {
		return nil, err
	}

	return c.replaceWithOptions(ctx, collection, docs, opts.(*ReplaceOptions))
}

func (c *driverCRUD) Update(ctx context.Context, collection string, filter Filter, fields Update, options ...*UpdateOptions) (*UpdateResponse, error) {
	opts, err := validateOptionsParam(options, &UpdateOptions{})
	if err != nil {
		return nil, err
	}

	return c.updateWithOptions(ctx, collection, filter, fields, opts.(*UpdateOptions))
}

func (c *driverCRUD) Delete(ctx context.Context, collection string, filter Filter, options ...*DeleteOptions) (*DeleteResponse, error) {
	opts, err := validateOptionsParam(options, &DeleteOptions{})
	if err != nil {
		return nil, err
	}

	return c.deleteWithOptions(ctx, collection, filter, opts.(*DeleteOptions))
}

func (c *driverCRUD) Read(ctx context.Context, collection string, filter Filter, fields Projection, options ...*ReadOptions) (Iterator, error) {
	opts, err := validateOptionsParam(options, &ReadOptions{})
	if err != nil {
		return nil, err
	}

	return c.readWithOptions(ctx, collection, filter, fields, opts.(*ReadOptions))
}

func (c *driverCRUD) Search(ctx context.Context, collection string, request *SearchRequest) (SearchResultIterator, error) {
	if request == nil {
		return nil, fmt.Errorf("API does accept nil Search Request")
	}
	return c.search(ctx, collection, request)
}

func (c *driverCRUD) CreateOrUpdateCollection(ctx context.Context, collection string, schema Schema, options ...*CollectionOptions) error {
	opts, err := validateOptionsParam(options, &CollectionOptions{})
	if err != nil {
		return err
	}

	return c.createOrUpdateCollectionWithOptions(ctx, collection, schema, opts.(*CollectionOptions))
}

func (c *driverCRUD) DropCollection(ctx context.Context, collection string, options ...*CollectionOptions) error {
	opts, err := validateOptionsParam(options, &CollectionOptions{})
	if err != nil {
		return err
	}

	return c.dropCollectionWithOptions(ctx, collection, opts.(*CollectionOptions))
}

func (c *driverCRUD) ListCollections(ctx context.Context, options ...*CollectionOptions) ([]string, error) {
	opts, err := validateOptionsParam(options, &CollectionOptions{})
	if err != nil {
		return nil, err
	}
	return c.listCollectionsWithOptions(ctx, opts.(*CollectionOptions))
}

func (c *driverCRUD) DescribeCollection(ctx context.Context, collection string, options ...*CollectionOptions) (*DescribeCollectionResponse, error) {
	opts, err := validateOptionsParam(options, &CollectionOptions{})
	if err != nil {
		return nil, err
	}

	return c.describeCollectionWithOptions(ctx, collection, opts.(*CollectionOptions))
}

func (c *driverCRUD) Events(ctx context.Context, collection string, options ...*EventsOptions) (EventIterator, error) {
	opts, err := validateOptionsParam(options, &EventsOptions{})
	if err != nil {
		return nil, err
	}

	return c.eventsWithOptions(ctx, collection, opts.(*EventsOptions))
}

func (c *driverCRUD) Publish(ctx context.Context, collection string, docs []Document, options ...*PublishOptions) (*PublishResponse, error) {
	opts, err := validateOptionsParam(options, &PublishOptions{})
	if err != nil {
		return nil, err
	}

	return c.publishWithOptions(ctx, collection, docs, opts.(*PublishOptions))
}

func (c *driverCRUD) Subscribe(ctx context.Context, collection string, options ...*SubscribeOptions) (Iterator, error) {
	opts, err := validateOptionsParam(options, &SubscribeOptions{})
	if err != nil {
		return nil, err
	}

	return c.subscribeWithOptions(ctx, collection, opts.(*SubscribeOptions))
}

func validateOptionsParam(options interface{}, out interface{}) (interface{}, error) {
	v := reflect.ValueOf(options)

	if (v.Kind() != reflect.Array && v.Kind() != reflect.Slice) || v.Len() > 1 {
		return nil, fmt.Errorf("API accepts no more then one options parameter")
	}

	if v.Len() < 1 {
		return out, nil
	}

	return v.Index(0).Interface(), nil
}

// NewDriver connect to Tigris at the specified URL
// URL should be in the form: {hostname}:{port}
func NewDriver(ctx context.Context, cfg *config.Driver) (Driver, error) {
	if cfg == nil {
		cfg = &config.Driver{}
	}
	if DefaultProtocol == GRPC {
		return NewGRPCClient(ctx, cfg.URL, cfg)
	} else if DefaultProtocol == HTTP {
		return NewHTTPClient(ctx, cfg.URL, cfg)
	}
	return nil, fmt.Errorf("unsupported protocol")
}
