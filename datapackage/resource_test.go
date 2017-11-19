package datapackage

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/frictionlessdata/datapackage-go/validator"
	"github.com/matryer/is"
)

func ExampleNewResourceWithDefaultRegistry() {
	res, _ := NewResourceWithDefaultRegistry(r1)
	fmt.Println(res.Name)
	// Output: res1
}

func TestNew(t *testing.T) {
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
			{"NameInvalidChar", map[string]interface{}{"name": "u*p", "path": "http://url.com"}},
			{"NameWithSpace", map[string]interface{}{"name": "u p", "path": "http://url.com"}},
			{"NameIsNotString", map[string]interface{}{"name": 1, "path": "http://url.com"}},
			{"SchemaAsInt", map[string]interface{}{"name": "name", "schema": 1, "path": "http://url.com"}},
			{"SchemaInvalidPath", map[string]interface{}{"name": "name", "schema": "/bar", "path": "http://url.com"}},
			{"InvalidProfile", map[string]interface{}{"name": "foo", "path": "foo.csv", "profile": 1}},
			{"DataAsStringNoMediatype", map[string]interface{}{"name": "foo", "data": "1,2\n3,4"}},
			{"DataInvalidType", map[string]interface{}{"name": "foo", "data": 1}},
		}
		for _, d := range data {
			t.Run(d.desc, func(t *testing.T) {
				t.Parallel()
				is := is.New(t)
				_, err := NewResource(d.d, validator.MustInMemoryRegistry())
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
				r, err := NewResource(d.descriptor, validator.MustInMemoryRegistry())
				is.NoErr(err)
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
			{"URL", map[string]interface{}{"name": "foo", "url": "http://data/foo.csv"}, []string{"http://url.com"}},
			{"FilePath", map[string]interface{}{"name": "foo", "path": "data/foo.csv"}, []string{"data/foo.csv"}},
			{"SlicePath", map[string]interface{}{"name": "foo", "path": []string{"https://foo.csv", "http://data/bar.csv"}}, []string{"https://foo.csv", "http://data/bar.csv"}},
		}
		for _, d := range data {
			t.Run(d.testDescription, func(t *testing.T) {
				t.Parallel()
				is := is.New(t)
				r, err := NewResource(d.descriptor, validator.MustInMemoryRegistry())
				is.NoErr(err)
				is.True(reflect.DeepEqual(d.want, r.Path))
			})
		}
	})
	t.Run("ValidData", func(t *testing.T) {
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
			{
				"Table",
				map[string]interface{}{"name": "foo", "data": []interface{}{[]string{"A", "B"}, []string{"a", "b"}}},
				[]interface{}{[]string{"A", "B"}, []string{"a", "b"}},
			},
		}
		for _, d := range data {
			t.Run(d.testDescription, func(t *testing.T) {
				t.Parallel()
				is := is.New(t)
				r, err := NewResource(d.descriptor, validator.MustInMemoryRegistry())
				is.NoErr(err)
				if !reflect.DeepEqual(reflect.ValueOf(d.want).Interface(), r.Data) {
					t.Fatalf("want:%v type:%v got:%v type:%v", d.want, reflect.TypeOf(d.want), r.Data, reflect.TypeOf(r.Data))
				}
			})
		}
	})
}

func TestResource_Descriptor(t *testing.T) {
	is := is.New(t)
	r, err := NewResource(r1, validator.MustInMemoryRegistry())
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

func TestResource_Update(t *testing.T) {
	t.Run("Valid", func(t *testing.T) {
		is := is.New(t)
		r, err := NewResource(r1, validator.MustInMemoryRegistry())
		is.NoErr(err)
		is.NoErr(r.Update(r2, validator.InMemoryLoader()))
		desc, _ := r.Descriptor()
		is.Equal(desc, r2Filled)
	})
	t.Run("Invalid", func(t *testing.T) {
		is := is.New(t)
		r, err := NewResource(r1, validator.MustInMemoryRegistry())
		is.NoErr(err)
		if err := r.Update(invalidResource, validator.InMemoryLoader()); err == nil {
			t.Fatalf("want:err got:nil")
		}
	})
}
func TestResource_Tabular(t *testing.T) {
	is := is.New(t)
	r, _ := NewUncheckedResource(map[string]interface{}{"profile": "tabular-data-resource"})
	is.True(r.Tabular())
	r1, _ := NewUncheckedResource(map[string]interface{}{"profile": "data-resource"})
	is.True(!r1.Tabular())
}
