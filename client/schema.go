package client

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/tigrisdata/tigrisdb-client-go/driver"
)

const (
	tagName       = "tigrisdb"
	tagPrimaryKey = "primary_key"
	tagPII        = "pii"
	tagEncrypted  = "encrypted"
	tagSkip       = "-"
)

type SchemaField struct {
	Name   string        `json:"name,omitempty"`
	Type   string        `json:"type,omitempty"`
	Tags   []string      `json:"tags,omitempty"`
	Desc   string        `json:"description,omitempty"`
	Fields []SchemaField `json:"properties,omitempty"`
}

type Schema struct {
	Fields     []SchemaField `json:"properties,omitempty"`
	PrimaryKey []string      `json:"primary_key,omitempty"`
}

func modelName(s interface{}) string {
	t := reflect.TypeOf(s)
	return t.Name()
}

// TODO: Handle UUID, Datetime, Set types
func translateType(t reflect.Type) (string, error) {
	switch t.Kind() {
	case reflect.Struct:
		return "object", nil
	case reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			return "bytes", nil
		}
		return "array", nil
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return "bytes", nil
		}
		return "array", nil
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int:
		return "int", nil
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint:
		return "int", nil
	case reflect.Int64:
		return "bigint", nil
	case reflect.Uint64:
		return "bigint", nil
	case reflect.String:
		return "string", nil
	case reflect.Float32, reflect.Float64:
		return "double", nil
	case reflect.Bool:
		return "bool", nil
	case reflect.Map:
		return "map", nil
	}

	return "", fmt.Errorf("unsupported type: '%s'", t.Name())
}

func MarshalSchema(sch *Schema) (driver.Schema, error) {
	b, err := json.Marshal(sch)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func parsePKIndex(tag string) (int, error) {
	pks := strings.Split(tag, ":")
	i := 1
	if len(pks) > 1 {
		if len(pks) > 2 {
			return 0, fmt.Errorf("only one colon allowed in the tag")
		}
		var err error
		i, err = strconv.Atoi(pks[1])
		if err != nil {
			return 0, fmt.Errorf("error parsing primary key index %s", err)
		}
		if i == 0 {
			return 0, fmt.Errorf("primary key index starts from 1")
		}
	}

	return i, nil
}

func parseTag(name string, tag string, tags *[]string, pk map[string]int) (bool, error) {
	tlist := strings.Split(tag, ",")

	for _, tag := range tlist {
		if strings.Compare(tag, tagSkip) == 0 {
			return true, nil
		}
	}

	for _, tag := range tlist {
		if strings.HasPrefix(tag, tagPrimaryKey) {
			i, err := parsePKIndex(tag)
			if err != nil {
				return false, err
			}
			pk[name] = i
		} else if strings.Compare(tag, tagPII) == 0 {
			*tags = append(*tags, tagPII)
		} else if strings.Compare(tag, tagEncrypted) == 0 {
			*tags = append(*tags, tagEncrypted)
		}
	}

	return false, nil
}

func traverseFields(prefix string, t reflect.Type, pk map[string]int, nFields *int) ([]SchemaField, error) {
	var fields []SchemaField

	if prefix != "" {
		prefix += "."
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		var f SchemaField

		name := strings.Split(field.Tag.Get("json"), ",")[0]

		// Obey JSON skip tag
		if strings.Compare(name, tagSkip) == 0 {
			continue
		}

		// Use json name if it's defined
		if name == "" {
			name = field.Name
		}

		f.Name = name

		skip, err := parseTag(prefix+f.Name, field.Tag.Get(tagName), &f.Tags, pk)
		if err != nil {
			return nil, err
		}

		if skip {
			continue
		}

		f.Type, err = translateType(field.Type)
		if err != nil {
			return nil, err
		}

		if field.Type.Kind() == reflect.Struct {
			f.Fields, err = traverseFields(prefix+f.Name, field.Type, pk, nFields)
			if err != nil {
				return nil, err
			}
		}

		fields = append(fields, f)
		*nFields++
	}

	return fields, nil
}

func structToSchema(s interface{}) (*Schema, error) {
	t := reflect.TypeOf(s)
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("model should be of struct type")
	}

	var err error
	var sch Schema
	var nFields int
	pk := make(map[string]int)

	sch.Fields, err = traverseFields("", t, pk, &nFields)
	if err != nil {
		return nil, err
	}

	sch.PrimaryKey = make([]string, len(pk))
	for k, v := range pk {
		if v > nFields {
			return nil, fmt.Errorf("maximum primary key index is %v", nFields)
		}
		if v > len(pk) {
			return nil, fmt.Errorf("gap in the primary key index")
		}
		if sch.PrimaryKey[v-1] != "" {
			return nil, fmt.Errorf("duplicate primary key index")
		}
		sch.PrimaryKey[v-1] = k
	}

	if len(pk) == 0 {
		return nil, fmt.Errorf("no primary key defined in schema")
	}

	return &sch, nil
}
