package main

import (
	"fmt"

	"github.com/frictionlessdata/datapackage-go/datapackage"
	"github.com/frictionlessdata/datapackage-go/validator"
)

func main() {
	descriptor := map[string]interface{}{
		"resources": []interface{}{
			map[string]interface{}{
				"name":    "books",
				"path":    "books.csv",
				"format":  "csv",
				"profile": "tabular-data-resource",
				"schema": map[string]interface{}{
					"fields": []interface{}{
						map[string]interface{}{"name": "author", "type": "string"},
						map[string]interface{}{"name": "title", "type": "string"},
						map[string]interface{}{"name": "year", "type": "integer"},
					},
				},
			},
		},
	}
	pkg, err := datapackage.New(descriptor, ".", validator.InMemoryLoader())
	if err != nil {
		panic(err)
	}
	pkg.RemoveResource("books")
	pkg.AddResource(map[string]interface{}{
		"name":    "cities",
		"path":    "cities.csv",
		"format":  "csv",
		"profile": "tabular-data-resource",
		"schema": map[string]interface{}{
			"fields": []interface{}{
				map[string]interface{}{"name": "author", "type": "string"},
				map[string]interface{}{"name": "title", "type": "string"},
				map[string]interface{}{"name": "year", "type": "integer"},
			},
		},
	})
	cities, _ := pkg.GetResource("cities").ReadAll()
	fmt.Println("## Cities: ", cities)
}
