package validator

import (
	"fmt"
	"strings"

	"github.com/xeipuuv/gojsonschema"
)

// DescriptorValidator validates a Data-Package or Resource descriptor.
type DescriptorValidator interface {
	IsValid(map[string]interface{}) bool
	Errors() []error
}

const localRegistryPath = "/registry.json"
const remoteRegistryURL = "http://frictionlessdata.io/schemas/registry.json"

// NewRegistry returns a registry where users could get descriptor validators.
func NewRegistry(loaders ...RegistryLoader) (Registry, error) {
	// Default settings.
	if len(loaders) == 0 {
		loaders = append(
			loaders,
			LocalRegistryLoader(localRegistryPath, false /* inMemoryOnly*/),
			RemoteRegistryLoader(remoteRegistryURL))
	}
	registry, err := FallbackRegistryLoader(loaders...)()
	if err != nil {
		return nil, fmt.Errorf("could not load registry:%q", err)
	}
	return registry, nil
}

// New returns a new descriptor validator for the passed-in profile.
func New(profile string, loaders ...RegistryLoader) (DescriptorValidator, error) {
	// If it is a third-party schema. Directly referenced from the internet or local file.
	if strings.HasPrefix(profile, "http") || strings.HasPrefix(profile, "file") {
		schema, err := gojsonschema.NewSchema(gojsonschema.NewReferenceLoader(profile))
		if err != nil {
			return nil, err
		}
		return &jsonSchema{schema: schema}, nil
	}
	registry, err := NewRegistry(loaders...)
	if err != nil {
		return nil, err
	}
	return registry.GetValidator(profile)
}

// IsValid checks the passed-in descriptor against the passed-in profile.
func IsValid(profile string, descriptor map[string]interface{}, loaders ...RegistryLoader) bool {
	validator, err := New(profile, loaders...)
	if err != nil {
		return false
	}
	return validator.IsValid(descriptor)
}

// Validate checks whether the descriptor the descriptor is valid against the passed-in profile/registry.
// If the validation process generates multiple errors, their messages are coalesced.
// It is a syntax-sugar around getting the validator from the registry and coalescing errors.
func Validate(descriptor map[string]interface{}, profile string, registry Registry) error {
	validator, err := registry.GetValidator(profile)
	if err != nil {
		return err
	}
	if !validator.IsValid(descriptor) {
		var erroMsg string
		for _, err := range validator.Errors() {
			erroMsg += fmt.Sprintln(err.Error())
		}
		return fmt.Errorf(erroMsg)
	}
	return nil
}

// MustInMemoryRegistry returns the local cache registry, which is shipped with the library.
// It panics if there are errors retrieving the registry.
func MustInMemoryRegistry() Registry {
	reg, err := InMemoryLoader()()
	if err != nil {
		panic(err)
	}
	return reg
}

// InMemoryLoader returns a loader which points tothe local cache registry.
func InMemoryLoader() RegistryLoader {
	return LocalRegistryLoader(localRegistryPath, true /* in memory only*/)
}
