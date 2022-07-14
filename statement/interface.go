package statement

import "github.com/hwcer/cosmo/schema"

type Statement interface {
	DBName(name string) string
	Schema() *schema.Schema
	Projection() map[string]int
}
