package main

import (
	"fmt"

	"github.com/frictionlessdata/datapackage-go/datapackage"
)

func main() {
	d, err := datapackage.Load("https://raw.githubusercontent.com/frictionlessdata/example-data-packages/master/periodic-table/datapackage.json")
	if err != nil {
		panic(err)
	}
	r := d.GetResource("data")
	contents, err := r.ReadAll()
	if err != nil {
		panic(err)
	}
	fmt.Println(contents)
}
