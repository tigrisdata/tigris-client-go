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

package client

import (
	"context"
	"encoding/json"
	"reflect"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigrisdb-client-go/config"
	"github.com/tigrisdata/tigrisdb-client-go/driver"
)

type Client interface {
	Database(name string) Database
}

type Database interface {
	MigrateSchema(ctx context.Context, model interface{}, models ...interface{}) error
	Create(ctx context.Context) error
	Drop(ctx context.Context) error
	Insert(ctx context.Context, doc interface{}, docs ...interface{}) error
	Tx(ctx context.Context, fn func(tx Tx) error) error
}

type Tx interface {
	Insert(ctx context.Context, doc interface{}, docs ...interface{}) error
}

type client struct {
	driver driver.Driver
}

type database struct {
	*client
	name string
}

type tx struct {
	db *database
	tx driver.Tx
}

func NewClient(ctx context.Context, config *config.Config) (Client, error) {
	d, err := driver.NewDriver(ctx, config)
	if err != nil {
		return nil, err
	}
	return &client{d}, nil
}

func (c *client) Database(name string) Database {
	return &database{client: c, name: name}
}

func (db *database) MigrateSchema(ctx context.Context, model interface{}, models ...interface{}) error {
	//models parameter added to require at least one schema to migrate
	models = append(models, model)
	for _, m := range models {
		schema, err := structToSchema(m)
		if err != nil {
			return err
		}
		sch, err := MarshalSchema(schema)
		if err != nil {
			return err
		}
		coll := modelName(m)
		log.Debug().Interface("schema", schema).Str("collection", coll).Msg("MigrateSchema")
		if err = db.driver.AlterCollection(ctx, db.name, coll, sch, &driver.CollectionOptions{}); err != nil {
			return err
		}
	}
	return nil
}

func (db *database) Create(ctx context.Context) error {
	return db.driver.CreateDatabase(ctx, db.name, &driver.DatabaseOptions{})
}

func (db *database) Drop(ctx context.Context) error {
	return db.driver.DropDatabase(ctx, db.name, &driver.DatabaseOptions{})
}

// batchDocs batches docs into per collection arrays
func batchDocs(batch map[string][]driver.Document, docs []interface{}) error {
	for _, d := range docs {
		name := reflect.TypeOf(d).Name()
		b, err := json.Marshal(d)
		if err != nil {
			return err
		}

		batch[name] = append(batch[name], b)
	}

	return nil
}

// Insert one or more documents into collection
// Docs can be of different types (going to different collections)
func (db *database) Insert(ctx context.Context, doc interface{}, docs ...interface{}) error {
	docs = append(docs, doc)

	var bdocs map[string][]driver.Document

	if err := batchDocs(bdocs, docs); err != nil {
		return err
	}

	for k, v := range bdocs {
		_, err := db.driver.Insert(ctx, db.name, k, v)
		if err != nil {
			return err
		}
	}

	return nil
}

// Tx executes bunch of operations in a transaction
func (db *database) Tx(ctx context.Context, fn func(tx Tx) error) error {
	dtx, err := db.driver.BeginTx(ctx, db.name)
	if err != nil {
		return err
	}
	defer func() { _ = dtx.Rollback(ctx) }()

	tx := &tx{db, dtx}

	if err = fn(tx); err != nil {
		return err
	}

	return dtx.Commit(ctx)
}

// Insert one or more documents into collection in the transaction context
func (tx *tx) Insert(ctx context.Context, doc interface{}, docs ...interface{}) error {
	docs = append(docs, doc)

	var bdocs map[string][]driver.Document

	if err := batchDocs(bdocs, docs); err != nil {
		return err
	}

	for k, v := range bdocs {
		_, err := tx.db.driver.Insert(ctx, tx.db.name, k, v)
		if err != nil {
			return err
		}
	}

	return nil
}
