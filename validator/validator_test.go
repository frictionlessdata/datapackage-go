package validator

import "fmt"

func ExampleValidate() {
	resource := map[string]interface{}{"name": "foo", "path": "foo.csv"}
	fmt.Print(Validate(resource, "data-resource", MustInMemoryRegistry()))
	// Output: <nil>
}

func ExampleNewRegistry() {
	registry, _ := NewRegistry(LocalRegistryLoader(localRegistryPath, true /* in memory only*/))
	validator, _ := registry.GetValidator("data-resource")
	fmt.Println(validator.Validate(map[string]interface{}{"name": "res1", "path": "foo.csv"}))
	// Output: <nil>
}
