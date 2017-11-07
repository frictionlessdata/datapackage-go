package pkg

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/frictionlessdata/datapackage-go/resource"
)

const (
	resourcePropName = "resources"
)

type resourceFactory func(map[string]interface{}) (*resource.Resource, error)

// Pkg represents a https://specs.frictionlessdata.io/data-package/
type Pkg struct {
	resources map[string]*resource.Resource

	descriptor map[string]interface{}
}

// GetResource return the resource which the passed-in name or nil if the resource is not part of the package.
func (p *Pkg) GetResource(name string) *resource.Resource {
	return p.resources[name]
}

func fromDescriptor(descriptor map[string]interface{}, newResource resourceFactory) (*Pkg, error) {
	r, ok := descriptor[resourcePropName]
	if !ok {
		return nil, fmt.Errorf("resources property is required, with at least one resource")
	}
	rSlice, ok := r.([]interface{})
	if !ok || len(rSlice) == 0 {
		return nil, fmt.Errorf("resources property is required, with at least one resource")
	}
	resources := make(map[string]*resource.Resource)
	for _, rInt := range rSlice {
		rDesc, ok := rInt.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("resources must be a json object. got:%v", rInt)
		}
		r, err := newResource(rDesc)
		if err != nil {
			return nil, err
		}
		resources[r.Name] = r
	}
	return &Pkg{
		resources: resources,
	}, nil
}

// FromDescriptor creates a data package from a json descriptor.
func FromDescriptor(descriptor map[string]interface{}) (*Pkg, error) {
	return fromDescriptor(descriptor, resource.New)
}

func fromReader(r io.Reader, newResource resourceFactory) (*Pkg, error) {
	b, err := ioutil.ReadAll(bufio.NewReader(r))
	if err != nil {
		return nil, err
	}
	var descriptor map[string]interface{}
	if err := json.Unmarshal(b, &descriptor); err != nil {
		return nil, err
	}
	return fromDescriptor(descriptor, newResource)
}

// FromReader validates and returns a data package from an io.Reader.
func FromReader(r io.Reader) (*Pkg, error) {
	return fromReader(r, resource.New)
}
