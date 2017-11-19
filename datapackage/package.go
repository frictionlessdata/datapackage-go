package datapackage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"reflect"

	"github.com/frictionlessdata/datapackage-go/clone"
	"github.com/frictionlessdata/datapackage-go/validator"
)

const (
	resourcePropName              = "resources"
	profilePropName               = "profile"
	encodingPropName              = "encoding"
	defaultDataPackageProfile     = "data-package"
	defaultResourceEncoding       = "utf-8"
	defaultResourceProfile        = "data-resource"
	tabularDataPackageProfileName = "tabular-data-package"
)

// Package-specific factories: mostly used for making unit testing easier.
type resourceFactory func(map[string]interface{}) (*Resource, error)

// Package represents a https://specs.frictionlessdata.io/data-package/
type Package struct {
	resources []*Resource

	descriptor  map[string]interface{}
	valRegistry validator.Registry
}

// GetResource return the resource which the passed-in name or nil if the resource is not part of the package.
func (p *Package) GetResource(name string) (*Resource, bool) {
	for _, r := range p.resources {
		if r.Name == name {
			return r, true
		}
	}
	return nil, false
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
	resDesc, err := clone.Descriptor(d)
	if err != nil {
		return err
	}
	fillResourceDescriptorWithDefaultValues(resDesc)
	rSlice, ok := p.descriptor[resourcePropName].([]interface{})
	if !ok {
		return fmt.Errorf("invalid resources property:\"%v\"", p.descriptor[resourcePropName])
	}
	rSlice = append(rSlice, resDesc)
	r, err := buildResources(rSlice, p.valRegistry)
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
		r, err := buildResources(newSlice, p.valRegistry)
		if err != nil {
			return
		}
		p.descriptor[resourcePropName] = newSlice
		p.resources = r
	}
}

// Descriptor returns a deep copy of the underlying descriptor which describes the package.
func (p *Package) Descriptor() (map[string]interface{}, error) {
	return clone.Descriptor(p.descriptor)
}

// Update the package with the passed-in descriptor. The package will only be updated if the
// the new descriptor is valid, otherwise the error will be returned.
func (p *Package) Update(newDescriptor map[string]interface{}, loaders ...validator.RegistryLoader) error {
	newP, err := New(newDescriptor, loaders...)
	if err != nil {
		return err
	}
	*p = *newP
	return nil
}

// New creates a new data package based on the descriptor.
func New(descriptor map[string]interface{}, loaders ...validator.RegistryLoader) (*Package, error) {
	cpy, err := clone.Descriptor(descriptor)
	if err != nil {
		return nil, err
	}
	fillPackageDescriptorWithDefaultValues(cpy)
	profile, ok := cpy[profilePropName].(string)
	if !ok {
		return nil, fmt.Errorf("%s property MUST be a string", profilePropName)
	}
	registry, err := validator.NewRegistry(loaders...)
	if err != nil {
		return nil, err
	}
	if err := validator.Validate(cpy, profile, registry); err != nil {
		return nil, err
	}
	resources, err := buildResources(cpy[resourcePropName], registry)
	if err != nil {
		return nil, err
	}
	return &Package{
		resources:   resources,
		descriptor:  cpy,
		valRegistry: registry,
	}, nil
}

// FromReader creates a data package from an io.Reader.
func FromReader(r io.Reader, loaders ...validator.RegistryLoader) (*Package, error) {
	b, err := ioutil.ReadAll(bufio.NewReader(r))
	if err != nil {
		return nil, err
	}
	var descriptor map[string]interface{}
	if err := json.Unmarshal(b, &descriptor); err != nil {
		return nil, err
	}
	return New(descriptor, loaders...)
}

// LoadRemote downloads and parses a data package descriptor from the specified URL.
func LoadRemote(url string, loaders ...validator.RegistryLoader) (*Package, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return FromReader(resp.Body, loaders...)
}

func fillPackageDescriptorWithDefaultValues(descriptor map[string]interface{}) {
	if descriptor[profilePropName] == nil {
		descriptor[profilePropName] = defaultDataPackageProfile
	}
	rSlice, ok := descriptor[resourcePropName].([]interface{})
	if ok {
		for i := range rSlice {
			r, ok := rSlice[i].(map[string]interface{})
			if ok {
				fillResourceDescriptorWithDefaultValues(r)
			}
		}
	}
}

func buildResources(resI interface{}, reg validator.Registry) ([]*Resource, error) {
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
		r, err := NewResource(rDesc, reg)
		if err != nil {
			return nil, err
		}
		resources[pos] = r
	}
	return resources, nil
}
