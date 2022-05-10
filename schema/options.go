package schema

import (
	"sync"
)

//New 新封装schema Store Namer
func New(namer ...Namer) (i *Options) {
	opts := &Options{Store: &sync.Map{}}
	if len(namer) > 0 {
		opts.Namer = namer[0]
	} else {
		opts.Namer = &NamingStrategy{}
	}
	return opts
}

type Options struct {
	Namer
	Store *sync.Map
}
