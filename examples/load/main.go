package main

import (
	"encoding/json"
	"fmt"

	"github.com/frictionlessdata/datapackage-go/datapackage"
)

const datapackageURL = "https://raw.githubusercontent.com/frictionlessdata/datapackage-go/master/examples/datapackage_remote/datapackage.json"

func main() {
	pkg, err := datapackage.Load(datapackageURL)
	if err != nil {
		panic(err)
	}
	pkgDesc, _ := pkg.Descriptor()
	fmt.Printf("Data package \"%s\" successfully created.\n", pkgDesc["name"])

	fmt.Printf("## Resources ##")
	for _, res := range pkg.Resources() {
		d, _ := res.Descriptor()
		b, _ := json.MarshalIndent(d, "", "  ")
		fmt.Println(string(b))
	}

	fmt.Println("## Contents ##")
	books := pkg.GetResource("books")
	contents, _ := books.ReadAll()
	fmt.Println(contents)
}
