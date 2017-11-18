package pkg

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/matryer/is"
)

func TestNew(t *testing.T) {
	// Those tests are a mix of the JSONSchema validation of data-resources and checks of type
	// Avoiding touch the disk.
	useLocalSchemaFiles = false
	defer func() { useLocalSchemaFiles = true }()
	t.Run("Invalid", func(t *testing.T) {
		data := []struct {
			desc string
			d    map[string]interface{}
		}{
			{"EmptyDescriptor", map[string]interface{}{}},
			{"NoPathOrData", map[string]interface{}{"name": "foo"}},
			{"PathObject", map[string]interface{}{"name": "foo", "path": map[string]string{"foo": "bar"}}},
			{"AbsolutePath", map[string]interface{}{"name": "foo", "path": "/bar"}},
			{"InvalidRelativePath", map[string]interface{}{"name": "foo", "path": "../bar"}},
			{"InvalidSchemeURL", map[string]interface{}{"name": "foo", "path": "myscheme://bar"}},
			{"MixedPaths", map[string]interface{}{"name": "foo", "path": []string{"https://bar", "bar"}}},
			{"PathAndData", map[string]interface{}{"name": "foo", "data": "foo", "path": "foo"}},
			{"InvalidJSONStringData", map[string]interface{}{"name": "foo", "data": "invalidJSONObjectString"}},
			{"InvalidJSONType", map[string]interface{}{"name": "foo", "data": 1}},
			{"UpperCaseName", map[string]interface{}{"name": "UP", "path": "http://url.com"}},
			{"InvalidChar", map[string]interface{}{"name": "u*p", "path": "http://url.com"}},
			{"NameWithSpace", map[string]interface{}{"name": "u p", "path": "http://url.com"}},
			{"NameIsNotString", map[string]interface{}{"name": 1, "path": "http://url.com"}},
			{"SchemaAsInt", map[string]interface{}{"name": "name", "schema": 1, "path": "http://url.com"}},
			{"SchemaInvalidPath", map[string]interface{}{"name": "name", "schema": "/bar", "path": "http://url.com"}},
		}
		for _, d := range data {
			t.Run(d.desc, func(t *testing.T) {
				t.Parallel()
				is := is.New(t)
				_, err := NewResource(d.d)
				is.True(err != nil)
			})
		}
	})
	t.Run("ValidNames", func(t *testing.T) {
		data := []struct {
			testDescription string
			descriptor      map[string]interface{}
			want            string
		}{
			{"NoPunctuation", map[string]interface{}{"name": "up", "path": "foo.csv"}, "up"},
			{"WithPunctuation", map[string]interface{}{"name": "u.p_d.o.w.n", "path": "foo.csv"}, "u.p_d.o.w.n"},
		}
		for _, d := range data {
			t.Run(d.testDescription, func(t *testing.T) {
				t.Parallel()
				is := is.New(t)
				r, err := NewResource(d.descriptor)
				is.NoErr(err)
				fmt.Println(r.Name)
				is.True(r.Name == d.want)
			})
		}
	})
	t.Run("ValidPaths", func(t *testing.T) {
		data := []struct {
			testDescription string
			descriptor      map[string]interface{}
			want            []string
		}{
			{"URL", validResourceWithURL, []string{"http://url.com"}},
			{"FilePath", map[string]interface{}{"name": "foo", "path": "data/foo.csv"}, []string{"data/foo.csv"}},
			{"SlicePath", map[string]interface{}{"name": "foo", "path": []string{"https://foo.csv", "http://data/bar.csv"}}, []string{"https://foo.csv", "http://data/bar.csv"}},
		}
		for _, d := range data {
			t.Run(d.testDescription, func(t *testing.T) {
				t.Parallel()
				is := is.New(t)
				r, err := NewResource(d.descriptor)
				is.NoErr(err)
				is.True(reflect.DeepEqual(d.want, r.Path))
			})
		}
	})
	t.Run("ValidPaths", func(t *testing.T) {
		data := []struct {
			testDescription string
			descriptor      map[string]interface{}
			want            interface{}
		}{
			{
				"JSONObject",
				map[string]interface{}{"name": "foo", "data": map[string]interface{}{"a": 1, "b": 2}},
				map[string]interface{}{"a": 1, "b": 2},
			},
			{
				"JSONArray",
				map[string]interface{}{"name": "foo", "data": []interface{}{map[string]interface{}{"a": 1}, map[string]interface{}{"b": 2}}},
				[]interface{}{map[string]interface{}{"a": 1}, map[string]interface{}{"b": 2}},
			},
			{
				"String",
				map[string]interface{}{"name": "foo", "data": "A,B,C\n1,2,3\n4,5,6", "format": "csv"},
				"A,B,C\n1,2,3\n4,5,6",
			},
		}
		for _, d := range data {
			t.Run(d.testDescription, func(t *testing.T) {
				t.Parallel()
				is := is.New(t)
				r, err := NewResource(d.descriptor)
				is.NoErr(err)
				is.True(reflect.DeepEqual(d.want, r.Data))
			})
		}
	})
}

var validResourceWithURL = map[string]interface{}{"name": "foo", "path": "http://url.com"}

func TestResource_Descriptor(t *testing.T) {
	useLocalSchemaFiles = false
	defer func() { useLocalSchemaFiles = true }()

	is := is.New(t)
	r, err := NewResource(validResourceWithURL)
	is.NoErr(err)
	cpy, err := r.Descriptor()
	is.NoErr(err)
	is.Equal(r.descriptor, cpy)

	// Checking if modifying the copy would not affect the source.
	cpy["foo"] = "bar"
	if reflect.DeepEqual(r.descriptor, cpy) {
		t.Fatalf("%+v == %+v", r.descriptor, cpy)
	}
}

func TestResource_Valid(t *testing.T) {
	is := is.New(t)
	r, err := NewUncheckedResource(map[string]interface{}{})
	is.NoErr(err)
	if r.Valid() {
		t.Fatalf("%+v is not valid.", r.descriptor)
	}
}

func TestResource_Tabular(t *testing.T) {
	is := is.New(t)
	r, _ := NewUncheckedResource(map[string]interface{}{"profile": "tabular-data-resource"})
	is.True(r.Tabular())
	r1, _ := NewUncheckedResource(map[string]interface{}{"profile": "data-resource"})
	is.True(!r1.Tabular())
}
