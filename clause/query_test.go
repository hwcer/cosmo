package clause

import (
	"encoding/json"
	"testing"
)

func TestQuery(t *testing.T) {
	query := New()
	query.Where("_id", 130)
	query.Where("_id IN ? AND uid = ?", []int{110, 120}, "myUid")

	query.Where("_id = ? OR uid = ? OR iid IN ? ", 100, "GM", []int{1, 2, 3})

	query.Where("_id!=?", 180)

	query.Where("_id >= ?", 10)
	query.Where("name=?", "myname")

	s := query.Build(nil)
	b, _ := json.Marshal(s)
	t.Logf("%v", string(b))
}
