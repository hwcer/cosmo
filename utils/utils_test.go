package utils

import (
	"strings"
	"testing"
)

func TestIsValidDBNameChar(t *testing.T) {
	for _, db := range []string{"db", "dbName", "db_name", "db1", "1dbname", "db$name"} {
		if fields := strings.FieldsFunc(db, IsValidDBNameChar); len(fields) != 1 {
			t.Fatalf("failed to parse db name %v", db)
		}
	}
}

type mymap map[string]any

func TestToBson(t *testing.T) {
	m := mymap{}
	m["k"] = "2"
	if b, err := ToBson(m); err != nil {
		t.Logf("%v", err)
	} else {
		t.Logf("%v", b)
	}

}
