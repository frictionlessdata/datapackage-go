package datapackage

import (
	"encoding/json"
	"fmt"
	"net/url"
	"path"
	"path/filepath"
	"strings"

	"github.com/frictionlessdata/datapackage-go/clone"
	"github.com/frictionlessdata/datapackage-go/validator"
	"github.com/frictionlessdata/tableschema-go/csv"
	"github.com/frictionlessdata/tableschema-go/schema"
	"github.com/frictionlessdata/tableschema-go/table"
)

// Accepted tabular formats.
var tabularFormats = map[string]struct{}{
	"csv":  struct{}{},
	"tsv":  struct{}{},
	"xls":  struct{}{},
	"xlsx": struct{}{},
}

const (
	tabularDataResourceProfile = "tabular-data-resource"
)

type pathType byte

const (
	urlPath      pathType = 0
	relativePath pathType = 1
)

const (
	schemaProp    = "schema"
	nameProp      = "name"
	formatProp    = "format"
	mediaTypeProp = "mediatype"
	pathProp      = "path"
	dataProp      = "data"
	jsonFormat    = "json"
	profileProp   = "profile"
)

// Resource describes a data resource such as an individual file or table.
type Resource struct {
	descriptor map[string]interface{}
	path       []string
	data       interface{}
	name       string
	basePath   string
}

// Name returns the resource name.
func (r *Resource) Name() string {
	return r.name
}

// Descriptor returns a copy of the underlying descriptor which describes the resource.
func (r *Resource) Descriptor() map[string]interface{} {
	// Resource cescriptor is always valid. Don't need to make the interface overcomplicated.
	c, _ := clone.Descriptor(r.descriptor)
	return c
}

// Update the resource with the passed-in descriptor. The resource will only be updated if the
// the new descriptor is valid, otherwise the error will be returned.
func (r *Resource) Update(d map[string]interface{}, loaders ...validator.RegistryLoader) error {
	reg, err := validator.NewRegistry(loaders...)
	if err != nil {
		return err
	}
	res, err := NewResource(d, reg)
	if err != nil {
		return err
	}
	*r = *res
	return nil
}

// Tabular checks whether the resource is tabular.
func (r *Resource) Tabular() bool {
	if pStr, ok := r.descriptor[profileProp].(string); ok && pStr == tabularDataResourceProfile {
		return true
	}
	fStr, _ := r.descriptor[formatProp].(string)
	_, ok := tabularFormats[fStr]
	return ok
}

// GetTable returns a table object to access the data. Returns an error if the resource is not tabular.
func (r *Resource) GetTable(opts ...csv.CreationOpts) (table.Table, error) {
	if !r.Tabular() {
		return nil, fmt.Errorf("methods iter/read are not supported for non tabular data")
	}
	// Inlined resources.
	if r.data != nil {
		switch r.data.(type) {
		case string:
			return csv.NewTable(csv.FromString(r.data.(string)), opts...)
		default:
			return nil, fmt.Errorf("only csv and string is supported for inlining data")
		}
	}
	// Single-part resources.
	if len(r.path) == 1 {
		p := r.path[0]
		if r.basePath != "" {
			p = filepath.Join(r.basePath, p)
		}
		var source csv.Source
		if strings.HasPrefix(p, "http") {
			source = csv.Remote(p)
		} else {
			source = csv.FromFile(p)
		}
		t, err := csv.NewTable(source, opts...)
		if err != nil {
			return nil, err
		}
		return t, nil
	}
	return nil, nil
}

// ReadAll reads all rows from the table and return it as strings.
func (r *Resource) ReadAll(opts ...csv.CreationOpts) ([][]string, error) {
	t, err := r.GetTable(opts...)
	if err != nil {
		return nil, err
	}
	return t.ReadAll()
}

// Iter returns an Iterator to read the tabular resource. Iter returns an error
// if the table physical source can not be iterated.
// The iteration process always start at the beginning of the table.
func (r *Resource) Iter(opts ...csv.CreationOpts) (table.Iterator, error) {
	t, err := r.GetTable(opts...)
	if err != nil {
		return nil, err
	}
	return t.Iter()
}

// GetSchema returns the schema associated to the resource, if present. The returned
// schema is based on a copy of the descriptor. Changes to it won't affect the data package
// descriptor structure.
func (r *Resource) GetSchema() (schema.Schema, error) {
	if r.descriptor[schemaProp] == nil {
		return schema.Schema{}, fmt.Errorf("schema is not declared in the descriptor")
	}
	buf, err := json.Marshal(r.descriptor[schemaProp])
	if err != nil {
		return schema.Schema{}, err
	}
	var s schema.Schema
	json.Unmarshal(buf, &s)
	return s, nil
}

// Cast resource contents.
// The result argument must necessarily be the address for a slice. The slice
// may be nil or previously allocated.
func (r *Resource) Cast(out interface{}, opts ...csv.CreationOpts) error {
	sch, err := r.GetSchema()
	if err != nil {
		return err
	}
	tbl, err := r.GetTable(opts...)
	if err != nil {
		return err
	}
	return sch.CastTable(tbl, out)
}

// NewResourceWithDefaultRegistry creates a new Resource from the passed-in descriptor.
// It uses the default registry to validate the resource descriptor.
func NewResourceWithDefaultRegistry(d map[string]interface{}) (*Resource, error) {
	reg, err := validator.NewRegistry()
	if err != nil {
		return nil, err
	}
	return NewResource(d, reg)
}

// NewResource creates a new Resource from the passed-in descriptor, if valid. The
// passed-in validator.Registry will be the source of profiles used in the validation.
func NewResource(d map[string]interface{}, registry validator.Registry) (*Resource, error) {
	cpy, err := clone.Descriptor(d)
	if err != nil {
		return nil, err
	}
	fillResourceDescriptorWithDefaultValues(cpy)
	profile, ok := cpy[profilePropName].(string)
	if !ok {
		return nil, fmt.Errorf("profile property MUST be a string:\"%s\"", profilePropName)
	}
	if err := validator.Validate(cpy, profile, registry); err != nil {
		return nil, err
	}
	r := Resource{
		descriptor: cpy,
		name:       cpy[nameProp].(string),
	}
	pathI := cpy[pathProp]
	if pathI != nil {
		p, err := parsePath(pathI, cpy)
		if err != nil {
			return nil, err
		}
		r.path = append([]string{}, p...)
		return &r, nil
	}
	dataI := cpy[dataProp]
	data, err := parseData(dataI, cpy)
	if err != nil {
		return nil, err
	}
	r.data = data
	return &r, nil
}

func fillResourceDescriptorWithDefaultValues(r map[string]interface{}) {
	if r[profilePropName] == nil {
		r[profilePropName] = defaultResourceProfile
	}
	if r[encodingPropName] == nil {
		r[encodingPropName] = defaultResourceEncoding
	}
}

func parseData(dataI interface{}, d map[string]interface{}) (interface{}, error) {
	if dataI != nil {
		switch dataI.(type) {
		case string:
			if d[formatProp] == nil && d[mediaTypeProp] == nil {
				return nil, fmt.Errorf("format or mediatype properties MUST be provided for JSON data strings. Descriptor:%v", d)
			}
			return dataI, nil
		case []interface{}, map[string]interface{}:
			return dataI, nil
		}
	}
	return nil, fmt.Errorf("data property must be either a JSON array/object OR a JSON string. Descriptor:%v", d)
}

func parsePath(pathI interface{}, d map[string]interface{}) ([]string, error) {
	var returned []string
	// Parse.
	switch pathI.(type) {
	default:
		return nil, fmt.Errorf("path MUST be a string or an array of strings. Descriptor:%v", d)
	case string:
		if p, ok := pathI.(string); ok {
			returned = append(returned, p)
		}
	case []string:
		returned = append(returned, pathI.([]string)...)
	}
	var lastType, currType pathType
	// Validation.
	for index, p := range returned {
		// Check if it is a relative path.
		u, err := url.Parse(p)
		if err != nil || u.Scheme == "" {
			if path.IsAbs(p) || strings.HasPrefix(path.Clean(p), "..") {
				return nil, fmt.Errorf("absolute paths (/) and relative parent paths (../) MUST NOT be used. Descriptor:%v", d)
			}
			currType = relativePath
		} else { // Check if it is a valid URL.
			if u.Scheme != "http" && u.Scheme != "https" {
				return nil, fmt.Errorf("URLs MUST be fully qualified. MUST be using either http or https scheme. Descriptor:%v", d)
			}
			currType = urlPath
		}
		if index > 0 {
			if currType != lastType {
				return nil, fmt.Errorf("it is NOT permitted to mix fully qualified URLs and relative paths in a single resource. Descriptor:%v", d)
			}
			lastType = currType
		}
	}
	return returned, nil
}

// NewUncheckedResource returns an Resource instance based on the descriptor without any verification. The returned Resource might
// not be valid.
func NewUncheckedResource(d map[string]interface{}) *Resource {
	r := &Resource{descriptor: d}
	nI, ok := d["name"]
	if ok {
		nStr, ok := nI.(string)
		if ok {
			r.name = nStr
		}
	}
	return r
}

// NewResourceFromString creates a new Resource from the passed-in JSON descriptor, if valid. The
// passed-in validator.Registry will be the source of profiles used in the validation.
func NewResourceFromString(res string, registry validator.Registry) (*Resource, error) {
	var d map[string]interface{}
	if err := json.Unmarshal([]byte(res), &d); err != nil {
		return nil, err
	}
	fmt.Println(d)
	return NewResource(d, registry)
}
