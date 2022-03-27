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

	"github.com/tigrisdata/tigrisdb-client-go/config"
)

func ExampleDriver() {
	ctx := context.TODO()

	c, _ := NewDriver(ctx, &config.Config{URL: "localhost"})

	_ = c.CreateDatabase(ctx, "db1", &DatabaseOptions{})

	_ = c.CreateCollection(ctx, "db1", "coll1",
		Schema(`{ "properties": { "F1": { "type": "string" }, "F2": { "type": "string" } }, "primary_key": ["F1"] }`), &CollectionOptions{})

	_, _ = c.Insert(ctx, "db1", "c1", []Document{Document(`{"F1":"V1"}`)}, &InsertOptions{})

	it, _ := c.Read(ctx, "db1", "c1", Filter(`{"F1":"V1"}`), &ReadOptions{})

	var doc Document
	for it.Next(&doc) {
		fmt.Printf("doc: %v\n", doc)
	}

	_ = it.Err()

	_, _ = c.Delete(ctx, "db1", "c1", Filter(`{"F1":"V1"}`), &DeleteOptions{})

	tx, _ := c.BeginTx(ctx, "db1", nil)
	defer func() { _ = tx.Rollback(ctx) }()

	_, _ = tx.Insert(ctx, "c1", []Document{Document(`{"F1":"V1"}`)}, &InsertOptions{})

	it, _ = tx.Read(ctx, "c1", Filter(`{"F1":"V1"}`), &ReadOptions{})

	for it.Next(&doc) {
		fmt.Printf("doc: %v\n", doc)
	}

	_ = it.Err()

	_, _ = tx.Update(ctx, "c1", Filter(`{"F1":"V1"}`),
		Fields(`{"$set" : { "F2" : "V2"}}`), &UpdateOptions{})

	_, _ = tx.Delete(ctx, "c1", Filter(`{"F1":"V1"}`), &DeleteOptions{})

	_ = tx.Commit(ctx)

	_ = c.DropDatabase(ctx, "db1", &DatabaseOptions{})
}
