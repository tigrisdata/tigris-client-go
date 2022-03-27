package client

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigrisdb-client-go/api/server/v1"
	"github.com/tigrisdata/tigrisdb-client-go/config"
	"github.com/tigrisdata/tigrisdb-client-go/driver"
	"github.com/tigrisdata/tigrisdb-client-go/test"
	"google.golang.org/protobuf/proto"
)

func pm(m proto.Message) gomock.Matcher {
	return &test.ProtoMatcher{Message: m}
}

func TestClientSchemaMigration(t *testing.T) {
	mc, cancel := test.SetupTests(t)
	defer cancel()

	ctx, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel1()

	cfg := &config.Config{URL: "localhost:33334"}
	cfg.TLS = test.SetupTLS(t)

	driver.DefaultProtocol = driver.GRPC
	s, err := NewClient(ctx, cfg)
	require.NoError(t, err)

	db := s.Database("db1")

	type testSchema1 struct {
		Key1 string `json:"key_1" tigrisdb:"primary_key"`
	}

	type testSchema2 struct {
		Key2 string `json:"key_2" tigrisdb:"primary_key"`
	}

	mc.EXPECT().AlterCollection(gomock.Any(),
		pm(&api.AlterCollectionRequest{
			Db: "db1", Collection: "testSchema1",
			Schema:  []byte(`{"properties":[{"name":"key_1","type":"string"}],"primary_key":["key_1"]}`),
			Options: &api.CollectionOptions{},
		})).Do(func(ctx context.Context, r *api.AlterCollectionRequest) {
	}).Return(&api.AlterCollectionResponse{}, nil)

	err = db.MigrateSchema(ctx, testSchema1{})
	require.NoError(t, err)

	mc.EXPECT().AlterCollection(gomock.Any(),
		pm(&api.AlterCollectionRequest{
			Db: "db1", Collection: "testSchema1",
			Schema:  []byte(`{"properties":[{"name":"key_1","type":"string"}],"primary_key":["key_1"]}`),
			Options: &api.CollectionOptions{},
		})).Do(func(ctx context.Context, r *api.AlterCollectionRequest) {
	}).Return(&api.AlterCollectionResponse{}, nil)

	mc.EXPECT().AlterCollection(gomock.Any(),
		pm(&api.AlterCollectionRequest{
			Db: "db1", Collection: "testSchema2",
			Schema:  []byte(`{"properties":[{"name":"key_2","type":"string"}],"primary_key":["key_2"]}`),
			Options: &api.CollectionOptions{},
		})).Do(func(ctx context.Context, r *api.AlterCollectionRequest) {
	}).Return(&api.AlterCollectionResponse{}, nil)

	err = db.MigrateSchema(ctx, testSchema1{}, testSchema2{})
	require.NoError(t, err)

	var m map[string]string
	err = db.MigrateSchema(ctx, &m)
	require.Error(t, err)

	var i int
	err = db.MigrateSchema(ctx, &i)
	require.Error(t, err)
}

func TestClientSchema(t *testing.T) {
	// invalid
	type empty struct {
	}

	// invalid
	type noPK struct {
		Key1 string `json:"key_1"`
	}

	// valid. implicit one part primary key with index 1
	type pk struct {
		Key1 string `json:"key_1" tigrisdb:"primary_key"`
	}

	// invalid. index starts from 1
	type pk0 struct {
		Key1 string `json:"key_1" tigrisdb:"primary_key:0"`
	}

	// valid. optional index specified
	type pk1 struct {
		Key1 string `json:"key_1" tigrisdb:"primary_key:1"`
	}

	// valid. composite primary key
	type pk2 struct {
		Key2 string `json:"key_2" tigrisdb:"primary_key:2"`
		Key1 string `json:"key_1" tigrisdb:"primary_key:1"`
	}

	// invalid. promary key index greater then number of fields
	type pkOutOfBound struct {
		Key3 string `json:"key_3" tigrisdb:"primary_key:3"`
		Key1 string `json:"key_1" tigrisdb:"primary_key:1"`
	}

	// invalid. gat in primary key indexes
	type pkGap struct {
		Key3  string `json:"key_3" tigrisdb:"primary_key:3"`
		Key1  string `json:"key_1" tigrisdb:"primary_key:1"`
		Data1 string
	}

	type pk3 struct {
		Key2 string `json:"key_2" tigrisdb:"primary_key:2"`
		Key1 string `json:"key_1" tigrisdb:"primary_key:1"`
		Key3 string `json:"key_3" tigrisdb:"primary_key:3"`
	}

	type subSubStruct struct {
		Data1  int    `tigrisdb:"-"`
		Field1 string `json:"ss_field_1" tigrisdb:"primary_key:1"`
	}

	type subStruct struct {
		Field1 string `json:"field_1"`
		Nested subSubStruct
	}

	type allTypes struct {
		Int8   int8  `json:"int_8"`
		Int16  int16 `json:"int_16"`
		Int32  int32 `json:"int_32"`
		Uint8  int8  `json:"uint_8"`
		Uint16 int16 `json:"uint_16"`
		Uint32 int32 `json:"uint_32"`

		Int64  uint64 `json:"int_64"`
		Uint64 int64  `json:"uint_64"`
		Int    int    `json:"int_1"`
		Uint   int    `json:"uint_1"`

		Bytes  []byte  `json:"bytes_1"`
		BytesA [4]byte `json:"bytes_2"`

		Float32 float32 `json:"float_32"`
		Float64 float64 `json:"float_64"`

		Bool bool `json:"bool_1"`

		String string `json:"string_1" tigrisdb:"primary_key:2"`

		Data1  subStruct            `json:"data_1"`
		Slice1 []string             `json:"slice_1"`
		Arr1   [3]string            `json:"arr_1"`
		Map    map[string]string    `json:"map_1"`
		Slice2 []subStruct          `json:"slice_2"`
		Map2   map[string]subStruct `json:"map_2"`
		Data2  subStruct            `json:"data_2" tigrisdb:"primary_key:3,-"` // should be skipped

		Bool_123 bool

		DataSkipped int `json:"-"`
	}

	cases := []struct {
		input  interface{}
		output *Schema
		err    error
	}{
		{empty{}, nil, fmt.Errorf("no primary key defined in schema")},
		{noPK{}, nil, fmt.Errorf("no primary key defined in schema")},
		{pk0{}, nil, fmt.Errorf("primary key index starts from 1")},
		{pkOutOfBound{}, nil, fmt.Errorf("maximum primary key index is 2")},
		{pkGap{}, nil, fmt.Errorf("gap in the primary key index")},
		{pk{}, &Schema{Fields: []SchemaField{
			{Name: "key_1", Type: "string"}}, PrimaryKey: []string{"key_1"}}, nil},
		{pk1{}, &Schema{Fields: []SchemaField{
			{Name: "key_1", Type: "string"}}, PrimaryKey: []string{"key_1"}}, nil},
		{pk2{}, &Schema{Fields: []SchemaField{
			{Name: "key_2", Type: "string"}, {Name: "key_1", Type: "string"}},
			PrimaryKey: []string{"key_1", "key_2"}}, nil},
		{pk3{}, &Schema{Fields: []SchemaField{
			{Name: "key_2", Type: "string"}, {Name: "key_1", Type: "string"}, {Name: "key_3", Type: "string"}},
			PrimaryKey: []string{"key_1", "key_2", "key_3"}}, nil},
		{allTypes{}, &Schema{Fields: []SchemaField{
			{Name: "int_8", Type: "int"},
			{Name: "int_16", Type: "int"},
			{Name: "int_32", Type: "int"},
			{Name: "uint_8", Type: "int"},
			{Name: "uint_16", Type: "int"},
			{Name: "uint_32", Type: "int"},

			{Name: "int_64", Type: "bigint"},
			{Name: "uint_64", Type: "bigint"},
			{Name: "int_1", Type: "int"},
			{Name: "uint_1", Type: "int"},

			{Name: "bytes_1", Type: "bytes"},
			{Name: "bytes_2", Type: "bytes"},

			{Name: "float_32", Type: "double"},
			{Name: "float_64", Type: "double"},

			{Name: "bool_1", Type: "bool"},

			{Name: "string_1", Type: "string"},

			{
				Name: "data_1",
				Type: "object",
				Fields: []SchemaField{
					{
						Name: "field_1", Type: "string"},
					{
						Name: "Nested",
						Type: "object",
						Fields: []SchemaField{
							{Name: "ss_field_1", Type: "string"},
						},
					},
				},
			},

			{Name: "slice_1", Type: "array"},
			{Name: "arr_1", Type: "array"},
			{Name: "map_1", Type: "map"},

			{Name: "slice_2", Type: "array"},
			{Name: "map_2", Type: "map"},

			// use original name if JSON tag name is not defined
			{Name: "Bool_123", Type: "bool"}},
			PrimaryKey: []string{"data_1.Nested.ss_field_1", "string_1"}}, nil},
	}

	for _, c := range cases {
		t.Run(reflect.TypeOf(c.input).Name(), func(t *testing.T) {
			schema, err := structToSchema(c.input)
			assert.Equal(t, c.err, err)
			assert.Equal(t, c.output, schema)
		})
	}
}
