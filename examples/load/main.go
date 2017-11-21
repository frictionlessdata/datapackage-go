package main

import (
	"encoding/json"
	"fmt"

	"github.com/frictionlessdata/datapackage-go/datapackage"
	"github.com/frictionlessdata/tableschema-go/csv"
)

func main() {
	pkg, err := datapackage.Load("datapackage.json")
	if err != nil {
		panic(err)
	}
	pkgDesc, _ := pkg.Descriptor()
	fmt.Printf("Data package \"%s\" successfully created.\n", pkgDesc["name"])

	fmt.Printf("\n## Resources ##")
	for _, res := range pkg.Resources() {
		d, _ := res.Descriptor()
		b, _ := json.MarshalIndent(d, "", "  ")
		fmt.Println(string(b))
	}

	fmt.Println("\n## Raw Content ##")
	books := pkg.GetResource("books")
	contents, _ := books.ReadAll()
	fmt.Println(contents)

	fmt.Println("\n## Cast Content ##")
	book := struct {
		Author string `tableheader:"author"`
		Title  string `tableheader:"title"`
		Year   int    `tableheader:"year"`
	}{}
	sch, _ := books.GetSchema()
	iter, _ := books.Iter(csv.LoadHeaders())
	for iter.Next() {
		sch.CastRow(iter.Row(), &book)
		fmt.Printf("%+v\n", book)
	}
}
