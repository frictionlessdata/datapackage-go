package pkg

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"reflect"

	"github.com/frictionlessdata/datapackage-go/clone"
)

const (
	resourcePropName              = "resources"
	profilePropName               = "profile"
	encodingPropName              = "encoding"
	defaultDataPackageProfile     = "data-package"
	defaultResourceEncoding       = "utf-8"
	tabularDataPackageProfileName = "tabular-data-package"
)

// Package-specific factories: mostly used for making unit testing easier.
type resourceFactory func(map[string]interface{}) (*Resource, error)
type validatorFactory func(string) (descriptorValidator, error)

// Package represents a https://specs.frictionlessdata.io/data-package/
type Package struct {
	resources []*Resource

	descriptor map[string]interface{}
	resFactory resourceFactory
	valFactory validatorFactory
}

// GetResource return the resource which the passed-in name or nil if the resource is not part of the package.
func (p *Package) GetResource(name string) *Resource {
	for _, r := range p.resources {
		if r.Name == name {
			return r
		}
	}
	return nil
}

// ResourceNames return a slice containing the name of the resources.
func (p *Package) ResourceNames() []string {
	s := make([]string, len(p.resources))
	for i, r := range p.resources {
		s[i] = r.Name
	}
	return s
}

// AddResource adds a new resource to the package, updating its descriptor accordingly.
func (p *Package) AddResource(d map[string]interface{}) error {
	if p.resFactory == nil {
		return fmt.Errorf("invalid resource factory. Did you mean resources.FromDescriptor?")
	}
	resDesc, err := clone.Descriptor(d)
	if err != nil {
		return err
	}
	rSlice, ok := p.descriptor[resourcePropName].([]interface{})
	if !ok {
		return fmt.Errorf("invalid resources property:\"%v\"", p.descriptor[resourcePropName])
	}
	rSlice = append(rSlice, resDesc)
	r, err := buildResources(rSlice, p.resFactory)
	if err != nil {
		return err
	}
	p.descriptor[resourcePropName] = rSlice
	p.resources = r
	return nil
}

//RemoveResource removes the resource from the package, updating its descriptor accordingly.
func (p *Package) RemoveResource(name string) {
	index := -1
	rSlice, ok := p.descriptor[resourcePropName].([]interface{})
	if !ok {
		return
	}
	for i := range rSlice {
		r := rSlice[i].(map[string]interface{})
		if r["name"] == name {
			index = i
			break
		}
	}
	if index > -1 {
		newSlice := append(rSlice[:index], rSlice[:index+1]...)
		r, err := buildResources(newSlice, p.resFactory)
		if err != nil {
			return
		}
		p.descriptor[resourcePropName] = newSlice
		p.resources = r
	}
}

// Valid returns true if the passed-in descriptor is valid or false, otherwise.
func Valid(descriptor map[string]interface{}) bool {
	return validateDescriptor(descriptor, newJSONSchemaValidator) == nil
}

// Descriptor returns a deep copy of the underlying descriptor which describes the package.
func (p *Package) Descriptor() (map[string]interface{}, error) {
	return clone.Descriptor(p.descriptor)
}

// Update the package with the passed-in descriptor. The package will only be update if the
// the new descriptor is valid, otherwise the error will be returned.
func (p *Package) Update(newDescriptor map[string]interface{}) error {
	newP, err := fromDescriptor(newDescriptor, p.resFactory, p.valFactory)
	if err != nil {
		return err
	}
	*p = *newP
	return nil
}

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

func buildResources(resI interface{}, resFactory resourceFactory) ([]*Resource, error) {
	rSlice, ok := resI.([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid resources property. Value:\"%v\" Type:\"%v\"", resI, reflect.TypeOf(resI))
	}
	resources := make([]*Resource, len(rSlice))
	for pos, rInt := range rSlice {
		rDesc, ok := rInt.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("resources must be a json object. got:%v", rInt)
		}
		r, err := resFactory(rDesc)
		if err != nil {
			return nil, err
		}
		resources[pos] = r
	}
	return resources, nil
}

func fromDescriptor(descriptor map[string]interface{}, resFactory resourceFactory, valFactory validatorFactory) (*Package, error) {
	cpy, err := clone.Descriptor(descriptor)
	if err != nil {
		return nil, err
	}
	fillDescriptorWithDefaultValues(cpy)
	if err := validateDescriptor(cpy, valFactory); err != nil {
		return nil, err
	}
	resources, err := buildResources(cpy[resourcePropName], resFactory)
	if err != nil {
		return nil, err
	}
	return &Package{
		resources:  resources,
		resFactory: resFactory,
		descriptor: descriptor,
		valFactory: valFactory,
	}, nil
}

func fillDescriptorWithDefaultValues(descriptor map[string]interface{}) {
	if descriptor[profilePropName] == nil {
		descriptor[profilePropName] = defaultDataPackageProfile
	}
	rSlice, ok := descriptor[resourcePropName].([]interface{})
	if ok {
		for i := range rSlice {
			r, ok := rSlice[i].(map[string]interface{})
			if ok {
				if r[profilePropName] == nil {
					r[profilePropName] = defaultResourceProfile
				}
				if r[encodingPropName] == nil {
					r[encodingPropName] = defaultResourceEncoding
				}
			}
		}
	}
}

// FromDescriptor creates a data package from a json descriptor.
func FromDescriptor(descriptor map[string]interface{}) (*Package, error) {
	return fromDescriptor(descriptor, NewResource, newJSONSchemaValidator)
}

func fromReader(r io.Reader, resFactory resourceFactory, valFactory validatorFactory) (*Package, error) {
	b, err := ioutil.ReadAll(bufio.NewReader(r))
	if err != nil {
		return nil, err
	}
	var descriptor map[string]interface{}
	if err := json.Unmarshal(b, &descriptor); err != nil {
		return nil, err
	}
	return fromDescriptor(descriptor, resFactory, valFactory)
}

// FromReader validates and returns a data package from an io.Reader.
func FromReader(r io.Reader) (*Package, error) {
	return fromReader(r, NewResource, newJSONSchemaValidator)
}
