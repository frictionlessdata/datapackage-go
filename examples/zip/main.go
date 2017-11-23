package main

import (
	"fmt"

	"github.com/frictionlessdata/datapackage-go/datapackage"
)

func main() {
	pkg, err := datapackage.Load("datapackage.json")
	if err != nil {
		panic(err)
	}
	fmt.Printf("Data package \"%s\" successfully created.\n", pkg.Descriptor()["name"])
	if err := pkg.Zip("package.zip"); err != nil {
		panic(err)
	}
	fmt.Println("Zip package.zip created successfully.")
}
