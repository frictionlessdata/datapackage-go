package datapackage

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/frictionlessdata/datapackage-go/validator"
	"github.com/matryer/is"
)

var invalidResource = map[string]interface{}{"name": "res1"}
var r1 = map[string]interface{}{"name": "res1", "path": "foo.csv"}
var r1Filled = map[string]interface{}{"name": "res1", "path": "foo.csv", "profile": "data-resource", "encoding": "utf-8"}
var r2 = map[string]interface{}{"name": "res2", "path": "bar.csv"}
var r2Filled = map[string]interface{}{"name": "res2", "path": "bar.csv", "profile": "data-resource", "encoding": "utf-8"}

func TestPackage_GetResource(t *testing.T) {
	is := is.New(t)
	pkg, err := New(map[string]interface{}{"resources": []interface{}{r1}}, validator.InMemoryLoader())
	is.NoErr(err)

	res, ok := pkg.GetResource("res1")
	is.True(ok)
	is.Equal(res.Name, "res1")

	res, ok = pkg.GetResource("foooooo")
	is.True(!ok)
	is.True(res == nil)
}

func TestPackage_AddResource(t *testing.T) {
	t.Run("ValidDescriptor", func(t *testing.T) {
		is := is.New(t)
		pkg, _ := New(map[string]interface{}{"resources": []interface{}{r1}}, validator.InMemoryLoader())
		is.NoErr(pkg.AddResource(r2))

		// Checking resources.
		is.Equal(len(pkg.resources), 2)
		is.Equal(pkg.resources[0].Name, "res1")
		is.Equal(pkg.resources[1].Name, "res2")

		// Checking descriptor.
		resDesc := pkg.descriptor["resources"].([]interface{})
		is.Equal(len(resDesc), 2)
		is.Equal(resDesc[0], r1Filled)
		is.Equal(resDesc[1], r2Filled)
	})
	t.Run("InvalidResource", func(t *testing.T) {
		pkg, _ := New(map[string]interface{}{"resources": []interface{}{r1}}, validator.InMemoryLoader())
		if err := pkg.AddResource(invalidResource); err == nil {
			t.Fatalf("want:err got:nil")
		}
	})
}

func TestPackage_RemoveResource(t *testing.T) {
	t.Run("Existing", func(t *testing.T) {
		is := is.New(t)
		pkg, _ := New(map[string]interface{}{"resources": []interface{}{r1, r2}}, validator.InMemoryLoader())
		pkg.RemoveResource("res1")

		resDesc := pkg.descriptor["resources"].([]interface{})
		is.Equal(len(resDesc), 1)
		is.Equal(resDesc[0], r1Filled)
		is.Equal(len(pkg.resources), 1)
		is.Equal(pkg.resources[0].Name, "res1")
	})
	t.Run("NonExisting", func(t *testing.T) {
		is := is.New(t)
		pkg, _ := New(map[string]interface{}{"resources": []interface{}{r1}}, validator.InMemoryLoader())
		pkg.RemoveResource("invalid")

		resDesc := pkg.descriptor["resources"].([]interface{})
		is.Equal(len(resDesc), 1)
		is.Equal(resDesc[0], r1Filled)
		is.Equal(len(pkg.resources), 1)
		is.Equal(pkg.resources[0].Name, "res1")
	})
}

func TestPackage_ResourceNames(t *testing.T) {
	is := is.New(t)
	pkg, _ := New(map[string]interface{}{"resources": []interface{}{r1, r2}}, validator.InMemoryLoader())
	is.Equal(pkg.ResourceNames(), []string{"res1", "res2"})
}

func TestPackage_Descriptor(t *testing.T) {
	is := is.New(t)
	pkg, _ := New(map[string]interface{}{"resources": []interface{}{r1}}, validator.InMemoryLoader())
	cpy, err := pkg.Descriptor()
	is.NoErr(err)
	is.Equal(pkg.descriptor, cpy)
}

func TestPackage_Update(t *testing.T) {
	t.Run("ValidResource", func(t *testing.T) {
		is := is.New(t)
		pkg, _ := New(map[string]interface{}{"resources": []interface{}{r1}}, validator.InMemoryLoader())
		newDesc := map[string]interface{}{"resources": []interface{}{r2}}
		is.NoErr(pkg.Update(newDesc, validator.InMemoryLoader()))
		d, _ := pkg.Descriptor()
		is.Equal(d, map[string]interface{}{"profile": "data-package", "resources": []interface{}{r2Filled}})
	})
	t.Run("InvalidResource", func(t *testing.T) {
		pkg, _ := New(map[string]interface{}{"resources": []interface{}{r1}}, validator.InMemoryLoader())
		newDesc := map[string]interface{}{"resources": []interface{}{invalidResource}}
		if err := pkg.Update(newDesc, validator.InMemoryLoader()); err == nil {
			t.Fatalf("want:err got:nil")
		}
	})
}

func TestFromDescriptor(t *testing.T) {
	t.Run("ValidationErrors", func(t *testing.T) {
		data := []struct {
			desc       string
			descriptor map[string]interface{}
		}{
			{"EmptyMap", map[string]interface{}{}},
			{"InvalidResourcePropertyType", map[string]interface{}{"resources": 10}},
			{"InvalidResource", map[string]interface{}{"resources": []interface{}{map[string]interface{}{}}}},
			{"InvalidResourceType", map[string]interface{}{"resources": []interface{}{1}}},
			{"ProfileNotAString", map[string]interface{}{"profile": 1, "resources": []interface{}{r1}}},
			{"ErrorCloning", map[string]interface{}{"profile": [][][]string{}, "resources": []interface{}{r1}}},
		}
		for _, d := range data {
			t.Run(d.desc, func(t *testing.T) {
				if _, err := New(d.descriptor, validator.InMemoryLoader()); err == nil {
					t.Fatalf("want:err got:nil")
				}
			})
		}
	})
	t.Run("ValidDescriptor", func(t *testing.T) {
		is := is.New(t)
		pkg, err := New(map[string]interface{}{"resources": []interface{}{r1}}, validator.InMemoryLoader())
		is.NoErr(err)
		is.Equal(len(pkg.resources), 1)
		is.Equal(pkg.resources[0].Name, "res1")
		resources := pkg.descriptor["resources"].([]interface{})
		is.Equal(len(resources), 1)
		is.Equal(resources[0], r1Filled)
	})
}

func TestFromReader(t *testing.T) {
	t.Run("ValidJSON", func(t *testing.T) {
		is := is.New(t)
		_, err := FromReader(strings.NewReader(`{"resources":[{"name":"res", "path":"foo.csv"}]}`), validator.InMemoryLoader())
		is.NoErr(err)
	})
	t.Run("InvalidJSON", func(t *testing.T) {
		is := is.New(t)
		_, err := FromReader(strings.NewReader(`{resources}`), validator.InMemoryLoader())
		is.True(err != nil)
	})
}

func TestLoadRemote(t *testing.T) {
	t.Run("ValidURL", func(t *testing.T) {
		is := is.New(t)
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintln(w, `{"resources":[{"name":"res", "path":"foo.csv"}]}`)
		}))
		defer ts.Close()
		pkg, err := LoadRemote(ts.URL, validator.InMemoryLoader())
		is.NoErr(err)
		res, _ := pkg.GetResource("res")
		is.Equal(res.Name, "res")
		is.Equal(res.Path, []string{"foo.csv"})
	})
	t.Run("ValidURL", func(t *testing.T) {
		_, err := LoadRemote("foobar", validator.InMemoryLoader())
		if err == nil {
			t.Fatalf("want:err got:nil")
		}
	})
}
