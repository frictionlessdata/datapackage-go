package datapackage

import (
	"archive/zip"
	"bufio"
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"

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
	descriptorFileNameWithinZip   = "datapackage.json"
)

// Needed to registry gob types.
// See FromReader for more details.
func init() {
	var i json.Number
	gob.Register(i)
}

// Package represents a https://specs.frictionlessdata.io/data-package/
type Package struct {
	resources []*Resource

	basePath    string
	descriptor  map[string]interface{}
	valRegistry validator.Registry
}

// GetResource return the resource which the passed-in name or nil if the resource is not part of the package.
func (p *Package) GetResource(name string) *Resource {
	for _, r := range p.resources {
		if r.name == name {
			return r
		}
	}
	return nil
}

// ResourceNames return a slice containing the name of the resources.
func (p *Package) ResourceNames() []string {
	s := make([]string, len(p.resources))
	for i, r := range p.resources {
		s[i] = r.name
	}
	return s
}

// Resources returns a copy of data package resources.
func (p *Package) Resources() []*Resource {
	// NOTE: Ignoring errors because we are not changing anything. Just cloning a valid package descriptor and building
	// its resources.
	cpy, _ := clone.Descriptor(p.descriptor)
	res, _ := buildResources(cpy[resourcePropName], p.basePath, p.valRegistry)
	return res
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
	r, err := buildResources(rSlice, p.basePath, p.valRegistry)
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
		newSlice := append(rSlice[:index], rSlice[index+1:]...)
		r, err := buildResources(newSlice, p.basePath, p.valRegistry)
		if err != nil {
			return
		}
		p.descriptor[resourcePropName] = newSlice
		p.resources = r
	}
}

// Descriptor returns a deep copy of the underlying descriptor which describes the package.
func (p *Package) Descriptor() map[string]interface{} {
	// Package cescriptor is always valid. Don't need to make the interface overcomplicated.
	c, _ := clone.Descriptor(p.descriptor)
	return c
}

// Update the package with the passed-in descriptor. The package will only be updated if the
// the new descriptor is valid, otherwise the error will be returned.
func (p *Package) Update(newDescriptor map[string]interface{}, loaders ...validator.RegistryLoader) error {
	newP, err := New(newDescriptor, p.basePath, loaders...)
	if err != nil {
		return err
	}
	*p = *newP
	return nil
}

func (p *Package) write(w io.Writer) error {
	b, err := json.MarshalIndent(p.descriptor, "", "  ")
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	if err != nil {
		return err
	}
	return nil
}

// SaveDescriptor saves the data package descriptor to the passed-in file path.
// It create creates the named file with mode 0666 (before umask), truncating
// it if it already exists.
func (p *Package) SaveDescriptor(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return p.write(f)
}

// Zip saves a zip-compressed file containing the package descriptor and all resource data.
// It create creates the named file with mode 0666 (before umask), truncating
// it if it already exists.
func (p *Package) Zip(path string) error {
	dir, err := ioutil.TempDir("", "datapackage_zip")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	// Saving descriptor.
	descriptorPath := filepath.Join(dir, descriptorFileNameWithinZip)
	if err := p.SaveDescriptor(descriptorPath); err != nil {
		return err
	}
	// Downloading resources.
	fPaths := []string{descriptorPath}
	for _, r := range p.resources {
		for _, p := range r.path {
			c, err := read(filepath.Join(r.basePath, p))
			if err != nil {
				return err
			}
			fDir := filepath.Join(dir, filepath.Dir(p))
			if err := os.MkdirAll(fDir, os.ModePerm); err != nil {
				return err
			}
			fPath := filepath.Join(fDir, filepath.Base(p))
			if err := ioutil.WriteFile(fPath, c, os.ModePerm); err != nil {
				return err
			}
			fPaths = append(fPaths, fPath)
		}
	}
	// Zipping everything.
	return zipFiles(path, dir, fPaths)
}

func zipFiles(filename string, basePath string, files []string) error {
	newfile, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer newfile.Close()
	zipWriter := zip.NewWriter(newfile)
	defer zipWriter.Close()
	for _, file := range files {
		zipfile, err := os.Open(file)
		if err != nil {
			return err
		}
		defer zipfile.Close()
		info, err := zipfile.Stat()
		if err != nil {
			return err
		}
		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return err
		}
		t := strings.TrimPrefix(strings.TrimPrefix(file, basePath), "/")
		if filepath.Dir(t) != "." {
			header.Name = t
		}
		writer, err := zipWriter.CreateHeader(header)
		if err != nil {
			return err
		}
		_, err = io.Copy(writer, zipfile)
		if err != nil {
			return err
		}
	}
	return nil
}

// New creates a new data package based on the descriptor.
func New(descriptor map[string]interface{}, basePath string, loaders ...validator.RegistryLoader) (*Package, error) {
	cpy, err := clone.Descriptor(descriptor)
	if err != nil {
		return nil, err
	}
	fillPackageDescriptorWithDefaultValues(cpy)
	loadPackageSchemas(cpy)
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
	resources, err := buildResources(cpy[resourcePropName], basePath, registry)
	if err != nil {
		return nil, err
	}
	return &Package{
		resources:   resources,
		descriptor:  cpy,
		valRegistry: registry,
		basePath:    basePath,
	}, nil
}

// FromReader creates a data package from an io.Reader.
func FromReader(r io.Reader, basePath string, loaders ...validator.RegistryLoader) (*Package, error) {
	// JSON doesn't differentiate between floats and integers. When parsed from JSON, large integers
	// get converted into scientific notation
	// Issue: https://github.com/frictionlessdata/datapackage-go/issues/28
	// Example at TestBigNumBytesIsValid.
	d := json.NewDecoder(bufio.NewReader(r))
	d.UseNumber()

	var descriptor map[string]interface{}
	if err := d.Decode(&descriptor); err != nil {
		return nil, err
	}
	return New(descriptor, basePath, loaders...)
}

// FromString creates a data package from a string representation of the package descriptor.
func FromString(in string, basePath string, loaders ...validator.RegistryLoader) (*Package, error) {
	return FromReader(strings.NewReader(in), basePath, loaders...)
}

// Load the data package descriptor from the specified URL or file path.
// If path has the ".zip" extension, it will be saved in local filesystem and decompressed before loading.
func Load(path string, loaders ...validator.RegistryLoader) (*Package, error) {
	contents, err := read(path)
	if err != nil {
		return nil, fmt.Errorf("error reading path contents (%s): %w", path, err)
	}
	if !strings.HasSuffix(path, ".zip") {
		return FromReader(bytes.NewBuffer(contents), getBasepath(path), loaders...)
	}
	// Special case for zip paths. BasePath will be the temporary directory.
	dir, err := ioutil.TempDir("", "datapackage_decompress")
	if err != nil {
		return nil, fmt.Errorf("error creating temporary directory: %w", err)
	}
	fNames, err := unzip(path, dir)
	if err != nil {
		return nil, fmt.Errorf("error unzipping path contents (%s): %w", path, err)
	}
	if _, ok := fNames[descriptorFileNameWithinZip]; ok {
		return Load(filepath.Join(dir, descriptorFileNameWithinZip), loaders...)
	}
	return nil, fmt.Errorf("zip file %s does not contain a file called %s", path, descriptorFileNameWithinZip)
}

func getBasepath(p string) string {
	if isRemotePath(p) {
		// If it is a remote-like URL, should not treat slashs in a system OS-dependent way.
		u, err := url.Parse(p)
		if err != nil {
			panic(err)
		}
		uStr := strings.TrimSuffix(u.String(), "/")
		uPath := strings.TrimSuffix(u.Path, "/")
		if uPath == "" {
			return fmt.Sprintf("%s/", uStr)
		}
		return strings.TrimSuffix(uStr, path.Base(u.String()))
	}
	return filepath.Dir(p)
}

func isRemotePath(x string) bool {
	m, e := regexp.MatchString(`^\w+://`, x)
	return e == nil && m
}

func read(path string) ([]byte, error) {
	if strings.HasPrefix(path, "http") {
		resp, err := http.Get(path)
		if err != nil {
			return nil, fmt.Errorf("error performing HTTP GET(%s): %w", path, err)
		}
		defer resp.Body.Close()
		buf, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("error reading response body contents (%s): %w", path, err)
		}
		return buf, nil
	}
	buf, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("error reading local file contents (%s): %w", path, err)
	}
	return buf, nil
}

func unzip(archive, basePath string) (map[string]struct{}, error) {
	fileNames := make(map[string]struct{})
	reader, err := zip.OpenReader(archive)
	if err != nil {
		return nil, fmt.Errorf("error opening zip reader(%s): %w", archive, err)
	}
	if err := os.MkdirAll(basePath, os.ModePerm); err != nil {
		return nil, fmt.Errorf("error creating directory (%s): %w", basePath, err)
	}
	for _, file := range reader.File {
		fileNames[file.Name] = struct{}{}
		path := filepath.Join(basePath, file.Name)
		if filepath.Dir(file.Name) != "." {
			dotDir := filepath.Join(basePath, filepath.Dir(file.Name))
			if err := os.MkdirAll(dotDir, os.ModePerm); err != nil {
				return nil, fmt.Errorf("error creating directory (%s): %w", dotDir, err)
			}
		}
		fileReader, err := file.Open()
		if err != nil {
			return nil, fmt.Errorf("error opening internal zip file (%s, %s): %w", archive, file.Name, err)
		}
		defer fileReader.Close()
		targetFile, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
		if err != nil {
			return nil, fmt.Errorf("error opening target external zip file (%s, %s): %w", archive, path, err)
		}
		defer targetFile.Close()
		if _, err := io.Copy(targetFile, fileReader); err != nil {
			return nil, fmt.Errorf("error filling target external zip file (%s, %s): %w", archive, path, err)
		}
	}
	return fileNames, nil
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

func loadPackageSchemas(d map[string]interface{}) error {
	var err error
	if schStr, ok := d[schemaProp].(string); ok {
		d[schemaProp], err = loadSchema(schStr)
		if err != nil {
			return err
		}
	}
	resources, _ := d[resourcePropName].([]interface{})
	for _, r := range resources {
		resMap, _ := r.(map[string]interface{})
		if schStr, ok := resMap[schemaProp].(string); ok {
			resMap[schemaProp], err = loadSchema(schStr)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func buildResources(resI interface{}, basePath string, reg validator.Registry) ([]*Resource, error) {
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
		r.basePath = basePath
		resources[pos] = r
	}
	return resources, nil
}
