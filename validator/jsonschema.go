package validator

import (
	"fmt"

	"github.com/xeipuuv/gojsonschema"
)

// jsonSchemaValidator is a validator backed by JSONSchema parsing and validation.
type jsonSchema struct {
	schema *gojsonschema.Schema
	errors []error
}

// IsValid checks the passed-in descriptor against the JSONSchema. If it returns
// false, erros can be checked calling Errors() method.
func (v *jsonSchema) IsValid(descriptor map[string]interface{}) bool {
	v.errors = nil
	result, err := v.schema.Validate(gojsonschema.NewGoLoader(descriptor))
	if err != nil {
		v.errors = append(v.errors, err)
		return false
	}
	for _, desc := range result.Errors() {
		v.errors = append(v.errors, fmt.Errorf(desc.String()))
	}
	return len(v.errors) == 0
}

// Errors returns the errors found at the last call of IsValid, if any.
func (v *jsonSchema) Errors() []error {
	return v.errors
}
