package main

import (
	"fmt"
	"strings"

	"github.com/frictionlessdata/datapackage-go/datapackage"
	"github.com/frictionlessdata/datapackage-go/validator"
)

func main() {
	in := `
	{
		"resources": [
	  	{
			"name": "example",
			"profile": "tabular-data-resource",
			"data": [
				["height", "age", "name"],
				["180", "18", "Tony"],
				["192", "32", "Jacob"]
			],
			"schema":  {
				"fields": [
					{"name": "height", "type": "integer"},
					{"name": "age", "type": "integer"},
					{"name": "name", "type": "string"}
				]
			}
		}
		]
 	}
`
	p, _ := datapackage.FromReader(strings.NewReader(in), validator.InMemoryLoader())
	res, _ := p.GetResource("example")
	fmt.Printf("Resource Name: \"%s\"\n", res.Name)
}
