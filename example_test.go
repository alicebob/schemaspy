package schemaspy_test

import (
	"fmt"
	"log"

	"github.com/alicebob/schemaspy"
)

func Example() {
	pgURL := "postgres://@localhost"
	schema, err := schemaspy.Public(pgURL)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("schema: %s\n", schema.Name)
	for _, t := range schema.Tables {
		table := schema.Relations[t]
		fmt.Printf("table: %s (%d cols)\n", t, len(table.Columns))
		for _, index := range table.Indexes {
			fmt.Printf("  index: %s\n", index)
		}
	}

}
