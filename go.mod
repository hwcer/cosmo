module github.com/hwcer/cosmo

go 1.16

replace (
	github.com/hwcer/cosgo v0.0.0 => ../cosgo
	github.com/hwcer/cosmo v0.0.0 => ./
)

require github.com/hwcer/cosgo v0.0.0

require (
	github.com/jinzhu/now v1.1.4
	go.mongodb.org/mongo-driver v1.9.1
)
