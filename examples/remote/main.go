package main

import (
	"fmt"

	"github.com/frictionlessdata/datapackage-go/datapackage"
	"github.com/frictionlessdata/tableschema-go/csv"
)

type element struct {
	Number int     `tableheader:"atomic number"`
	Symbol string  `tableheader:"symbol"`
	Name   string  `tableheader:"name"`
	Mass   float64 `tableheader:"atomic mass"`
	Metal  string  `tableheader:"metal or nonmetal?"`
}

func main() {
	pkg, err := datapackage.Load("https://raw.githubusercontent.com/frictionlessdata/example-data-packages/master/periodic-table/datapackage.json")
	if err != nil {
		panic(err)
	}
	resource := pkg.GetResource("data")
	var elements []element
	if err := resource.Cast(&elements, csv.LoadHeaders()); err != nil {
		panic(err)
	}
	for _, e := range elements {
		fmt.Printf("%+v\n", e)
	}
}
