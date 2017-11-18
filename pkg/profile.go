package pkg

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/frictionlessdata/datapackage-go/pkg/profile_cache"
	"github.com/xeipuuv/gojsonschema"
)

// descriptorValidator validates a Profile or Resource descriptor.
type descriptorValidator interface {
	IsValid(map[string]interface{}) bool
	Errors() []error
}

// jsonSchemaValidator is a validator backed by JSONSchema parsing and validation.
type jsonSchemaValidator struct {
	schema *gojsonschema.Schema
	errors []error
}

func (v *jsonSchemaValidator) IsValid(descriptor map[string]interface{}) bool {
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

func (v *jsonSchemaValidator) Errors() []error {
	return v.errors
}

// Fake validator is a lightweight descriptorValidator. Used in tests.
type fakeValidator struct {
	jsonSchemaValidator
}

func (v *fakeValidator) IsValid(_ map[string]interface{}) bool {
	return len(v.jsonSchemaValidator.errors) == 0
}

// validatorFactory returns a descritorValidator based on the passed-in profile ID.
type validatorFactory func(string) (descriptorValidator, error)

// newFakeValidator is a validator factory which returns fake validators. Used in tests.
func newFakeValidator(_ string) (descriptorValidator, error) { return &fakeValidator{}, nil }

func validateDescriptor(descriptor map[string]interface{}, valFactory validatorFactory) error {
	profile, ok := descriptor[profilePropName].(string)
	if !ok {
		return fmt.Errorf("%s property MUST be a string", profilePropName)
	}
	validator, err := valFactory(profile)
	if err != nil {
		return err
	}
	if !validator.IsValid(descriptor) {
		return fmt.Errorf("There are %d validation errors:%v", len(validator.Errors()), validator.Errors())
	}
	return nil
}

type profileSpec struct {
	ID            string `json:"id,omitempty"`
	Title         string `json:"title,omitempty"`
	Schema        string `json:"schema,omitempty"`
	SchemaPath    string `json:"schema_path,omitempty"`
	Specification string `json:"specification,omitempty"`
}

var registryLoader sync.Once
var schemaRegistry = map[string]profileSpec{}
var useLocalSchemaFiles = true

const localRegistryPath = "/registry.json"
const remoteRegistryURL = "http://frictionlessdata.io/schemas/registry.json"

func newJSONSchemaValidator(profile string) (descriptorValidator, error) {
	// Loading schema registry only once, at the first time it is needed.
	registryLoader.Do(func() {
		m, err := loadSchemaRegistry(useLocalSchemaFiles, localRegistryPath, remoteRegistryURL)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[warning] %q", err)
		}
		schemaRegistry = m
	})
	schema, err := loadSchema(schemaRegistry, profile, useLocalSchemaFiles)
	if err != nil {
		return nil, err
	}
	return &jsonSchemaValidator{schema: schema}, nil
}

func loadSchema(schemaRegistry map[string]profileSpec, profile string, useLocalSchemaFiles bool) (*gojsonschema.Schema, error) {
	// If it is a third-party schema. Directly referenced from the internet or local file.
	if strings.HasPrefix(profile, "http") || strings.HasPrefix(profile, "file") {
		schema, err := gojsonschema.NewSchema(gojsonschema.NewReferenceLoader(profile))
		if err != nil {
			return nil, err
		}
		return schema, nil
	}
	// If it is not, assume it is a ID from on of our the default registries: Data Package Schema Registry or local cache.
	spec, ok := schemaRegistry[profile]
	if !ok {
		return nil, fmt.Errorf("Invalid profile:%s", profile)
	}
	// Data Package Schema Registry.
	if strings.HasPrefix(spec.Schema, "http") {
		schema, err := gojsonschema.NewSchema(gojsonschema.NewReferenceLoader(spec.Schema))
		if err != nil {
			return nil, err
		}
		return schema, nil
	}
	// Local registry.
	b, err := profile_cache.FSByte(useLocalSchemaFiles, spec.Schema)
	if err != nil {
		return nil, err
	}
	schema, err := gojsonschema.NewSchema(gojsonschema.NewBytesLoader(b))
	if err != nil {
		return nil, err
	}
	return schema, nil
}

func loadSchemaRegistry(useLocalSchemaFiles bool, localRegistryPath, remoteRegistryURL string) (map[string]profileSpec, error) {
	// First attempt: fill out the registry contents from local file or resource bundle.
	registryContents, err := profile_cache.FSByte(useLocalSchemaFiles, localRegistryPath)
	if err != nil {
		// Second attempt: fill out the from the Data Package Schema Registry.
		resp, err := http.Get(remoteRegistryURL)
		if err != nil {
			return nil, fmt.Errorf("error fetching remote profile cache registry from %s. Err:%q\n", remoteRegistryURL, err)
		}
		defer resp.Body.Close()
		registryContents, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("error reading remote profile cache registry from %s. Err:%q\n", remoteRegistryURL, err)
		}
	}
	// Unmarshaling the slice of specs and creating a map.
	var specs []profileSpec
	if err := json.Unmarshal(registryContents, &specs); err != nil {
		return nil, fmt.Errorf("error parsing profile cache registry. Contents:\"%s\". Err:\"%q\"\n", string(registryContents), err)
	}
	m := make(map[string]profileSpec, len(specs))
	for _, s := range specs {
		m[s.ID] = s
	}
	return m, nil
}
