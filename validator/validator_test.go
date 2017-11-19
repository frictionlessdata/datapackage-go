package validator

import "fmt"

func ExampleIsValid() {
	resource := map[string]interface{}{"name": "foo", "path": "foo.csv"}
	loader := LocalRegistryLoader(localRegistryPath, true /* in memory only*/)
	fmt.Print(IsValid("data-resource", resource, loader))
	// Output: true
}

func ExampleNew() {
	validator, _ := New("data-resource")
	fmt.Println(validator.IsValid(map[string]interface{}{"name": "res1", "path": "foo.csv"}))
	// Output: true
}

func ExampleNewRegistry() {
	registry, _ := NewRegistry(LocalRegistryLoader(localRegistryPath, true /* in memory only*/))
	validator, _ := registry.GetValidator("data-resource")
	fmt.Println(validator.IsValid(map[string]interface{}{"name": "res1", "path": "foo.csv"}))
	// Output: true
}

func ExampleValidate() {
	registry, _ := NewRegistry(LocalRegistryLoader(localRegistryPath, true /* in memory only*/))
	fmt.Println(Validate(map[string]interface{}{"name": "res1", "path": "foo.csv"}, "data-resource", registry))
	// Output: <nil>
}
