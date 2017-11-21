package main

import (
	"encoding/json"
	"fmt"

	"github.com/frictionlessdata/datapackage-go/datapackage"
)

func main() {
	pkg, err := datapackage.Load("datapackage.json")
	if err != nil {
		panic(err)
	}
	fmt.Printf("Data package \"%s\" successfully created.\n", pkg.Descriptor()["name"])

	fmt.Printf("## Resources ##")
	for _, res := range pkg.Resources() {
		b, _ := json.MarshalIndent(res.Descriptor(), "", "  ")
		fmt.Println(string(b))
	}

	fmt.Println("## Contents ##")
	books := pkg.GetResource("books")
	contents, _ := books.ReadAll()
	fmt.Println(contents)
}
