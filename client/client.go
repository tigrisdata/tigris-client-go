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
}

type client struct {
	driver driver.Driver
}

type database struct {
	*client
	name string
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
