.PHONY: build setupdb int

build:
	go build -i

setupdb:
	psql schemaspy < test_schema.sql > /dev/null

int: setupdb
	go test -tags int -v ./...
