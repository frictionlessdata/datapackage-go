package pkg

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/frictionlessdata/datapackage-go/resource"
	"github.com/matryer/is"
)

func validResource(d map[string]interface{}) (*resource.Resource, error) {
	return &resource.Resource{Descriptor: d, Name: d["name"].(string)}, nil
}

var invalidResource = func(map[string]interface{}) (*resource.Resource, error) { return nil, fmt.Errorf("") }

func TestPackage_GetResource(t *testing.T) {
	is := is.New(t)
	in := `{"resources":[{"name":"res"}]}`
	p, err := fromReader(strings.NewReader(in), validResource)
	is.NoErr(err)
	is.Equal(p.GetResource("res").Name, "res")
	is.True(p.GetResource("foooooo") == nil)
}

func TestPackage_AddResource(t *testing.T) {
	t.Run("ValidDescriptor", func(t *testing.T) {
		is := is.New(t)
		r1 := map[string]interface{}{"name": "res1"}
		r2 := map[string]interface{}{"name": "res2"}

		p, err := fromDescriptor(map[string]interface{}{"resources": []interface{}{r1}}, validResource)
		is.NoErr(err)
		p.AddResource(r2)

		is.Equal(len(p.resources), 2)
		is.Equal(p.resources[1].Name, "res2")

		resources := p.descriptor["resources"].([]interface{})
		is.Equal(len(resources), 2)
		is.Equal(resources[0], r1)
		is.Equal(resources[1], r2)
	})
	t.Run("CodedPackage", func(t *testing.T) {
		is := is.New(t)
		p := Package{resFactory: validResource}
		r1 := map[string]interface{}{"name": "res1"}
		err := p.AddResource(r1)
		is.NoErr(err)

		resources := p.descriptor["resources"].([]interface{})
		is.Equal(len(resources), 1)
		is.Equal(resources[0], r1)

		is.Equal(len(p.resources), 1)
		is.Equal(p.resources[0].Name, "res1")
	})
	t.Run("InvalidResource", func(t *testing.T) {
		is := is.New(t)
		p := Package{resFactory: invalidResource}
		err := p.AddResource(map[string]interface{}{})
		is.True(err != nil)
	})
	t.Run("NoResFactory", func(t *testing.T) {
		is := is.New(t)
		p := Package{}
		err := p.AddResource(map[string]interface{}{"name": "res1"})
		is.True(err != nil)
	})
}

func TestPackage_RemoveResource(t *testing.T) {
	t.Run("ExistingName", func(t *testing.T) {
		is := is.New(t)
		p, err := fromDescriptor(
			map[string]interface{}{"resources": []interface{}{
				map[string]interface{}{"name": "res1"},
				map[string]interface{}{"name": "res2"},
			}},
			validResource)
		is.NoErr(err)
		p.RemoveResource("res1")
		is.Equal(len(p.descriptor), 1)
		is.Equal(len(p.resources), 1)
		is.Equal(p.descriptor["resources"].([]interface{})[0], p.resources[0].Descriptor)

		// Remove a non-existing resource and checks if everything stays the same.
		p.RemoveResource("res1")
		is.Equal(len(p.descriptor), 1)
		is.Equal(len(p.resources), 1)
		is.Equal(p.descriptor["resources"].([]interface{})[0], p.resources[0].Descriptor)
	})
}

func TestPackage_ResourceNames(t *testing.T) {
	is := is.New(t)
	p := Package{resFactory: validResource}
	is.True(p.AddResource(map[string]interface{}{"name": "res1"}) == nil)
	is.True(p.AddResource(map[string]interface{}{"name": "res2"}) == nil)
	is.Equal(p.ResourceNames(), []string{"res1", "res2"})
}

func TestPackage_Descriptor(t *testing.T) {
	is := is.New(t)
	p := Package{resFactory: validResource}
	is.True(p.AddResource(map[string]interface{}{"name": "res1"}) == nil)
	c, err := p.Descriptor()
	is.NoErr(err)
	is.Equal(p.descriptor, c)
}

func TestPackage_UnmarshalJSON(t *testing.T) {
	t.Run("ValidJSON", func(t *testing.T) {
		is := is.New(t)
		var p Package
		err := json.Unmarshal([]byte(`{"resources":[{"name":"res", "path":"foo.csv"}]}`), &p)
		is.NoErr(err)
		is.Equal(p.descriptor, map[string]interface{}{"resources": []interface{}{map[string]interface{}{"name": "res", "path": "foo.csv"}}})
	})
	t.Run("InvalidDescriptor", func(t *testing.T) {
		is := is.New(t)
		var p Package
		is.True(json.Unmarshal([]byte(`{"resources":1}`), &p) != nil)
	})
	t.Run("InvalidJSONMap", func(t *testing.T) {
		is := is.New(t)
		var p Package
		is.True(json.Unmarshal([]byte(`[]`), &p) != nil)
	})
}

func TestPackage_MarshalJSON(t *testing.T) {
	is := is.New(t)
	p := Package{resFactory: validResource}
	p.AddResource(map[string]interface{}{"name": "res", "path": "foo.csv"})
	buf, err := json.Marshal(&p)
	is.NoErr(err)
	is.Equal(string(buf), `{"resources":[{"name":"res","path":"foo.csv"}]}`)
}

func TestFromDescriptor(t *testing.T) {
	t.Run("ValidationErrors", func(t *testing.T) {
		is := is.New(t)
		data := []struct {
			desc       string
			descriptor map[string]interface{}
			resFactory resourceFactory
		}{
			{"EmptyMap", map[string]interface{}{}, validResource},
			{"InvalidResourcePropertyType", map[string]interface{}{
				"resources": 10,
			}, validResource},
			{"EmptyResourcesSlice", map[string]interface{}{
				"resources": []interface{}{},
			}, validResource},
			{"InvalidResource", map[string]interface{}{
				"resources": []interface{}{map[string]interface{}{}},
			}, invalidResource},
			{"InvalidResourceType", map[string]interface{}{
				"resources": []interface{}{1},
			}, validResource},
		}
		for _, d := range data {
			_, err := fromDescriptor(d.descriptor, d.resFactory)
			is.True(err != nil)
		}
	})
	t.Run("ValidDescriptor", func(t *testing.T) {
		is := is.New(t)
		r1 := map[string]interface{}{"name": "res"}
		p, err := fromDescriptor(
			map[string]interface{}{"resources": []interface{}{r1}},
			validResource,
		)
		is.NoErr(err)
		is.True(p.resources[0] != nil)

		resources := p.descriptor["resources"].([]interface{})
		is.Equal(len(resources), 1)
		is.Equal(r1, resources[0])
	})
}

func TestFromReader(t *testing.T) {
	t.Run("ValidJSON", func(t *testing.T) {
		is := is.New(t)
		_, err := fromReader(strings.NewReader(`{"resources":[{"name":"res"}]}`), validResource)
		is.NoErr(err)
	})
	t.Run("InvalidJSON", func(t *testing.T) {
		is := is.New(t)
		_, err := fromReader(strings.NewReader(`{resources}`), validResource)
		is.True(err != nil)
	})
}

func TestValid(t *testing.T) {
	is := is.New(t)
	is.True(valid(map[string]interface{}{"resources": []interface{}{map[string]interface{}{"name": "res"}}}, validResource))
	is.True(!valid(map[string]interface{}{}, validResource))
}
