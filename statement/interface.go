package statement

import "github.com/hwcer/cosgo/schema"

type Statement interface {
	DBName(name string) string
	Schema() *schema.Schema
	Projection() map[string]int
}
