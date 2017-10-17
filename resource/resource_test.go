package resource

import (
	"reflect"
	"testing"

	"github.com/matryer/is"
)

func TestNew_error(t *testing.T) {
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
		{"SchemaAsInt", map[string]interface{}{"name": 1, "schema": 1, "path": "http://url.com"}},
	}
	for _, d := range data {
		t.Run(d.desc, func(t *testing.T) {
			is := is.New(t)
			_, err := New(d.d)
			is.True(err != nil)
		})
	}
}

func TestNew_schema(t *testing.T) {
	data := []struct {
		testDescription string
		descriptor      map[string]interface{}
		want            interface{}
	}{
		{"StringSchemaPath", map[string]interface{}{"name": "up", "schema": "schema.json", "path": "foo.csv"}, "schema.json"},
		{"URLSchemaPath", map[string]interface{}{"name": "up", "schema": "http://schema.json", "path": "foo.csv"}, "http://schema.json"},
		{"ObjectSchema", map[string]interface{}{"name": "up", "schema": map[string]interface{}{"boo": "bez"}, "path": "foo.csv"}, map[string]interface{}{"boo": "bez"}},
	}
	for _, d := range data {
		t.Run(d.testDescription, func(t *testing.T) {
			is := is.New(t)
			r, err := New(d.descriptor)
			is.NoErr(err)
			is.True(reflect.DeepEqual(d.want, r.Descriptor["schema"]))
		})
	}
}

func TestNew_name(t *testing.T) {
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
			is := is.New(t)
			r, err := New(d.descriptor)
			is.NoErr(err)
			is.True(r.Name == d.want)
		})
	}
}

func TestNew_path(t *testing.T) {
	data := []struct {
		testDescription string
		descriptor      map[string]interface{}
		want            []string
	}{
		{"URL", map[string]interface{}{"name": "foo", "path": "http://url.com"}, []string{"http://url.com"}},
		{"FilePath", map[string]interface{}{"name": "foo", "path": "data/foo.csv"}, []string{"data/foo.csv"}},
		{"SlicePath", map[string]interface{}{"name": "foo", "path": []string{"https://foo.csv", "http://data/bar.csv"}}, []string{"https://foo.csv", "http://data/bar.csv"}},
	}
	for _, d := range data {
		t.Run(d.testDescription, func(t *testing.T) {
			is := is.New(t)
			r, err := New(d.descriptor)
			is.NoErr(err)
			is.True(reflect.DeepEqual(d.want, r.Path))
		})
	}
}

func TestNew_data(t *testing.T) {
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
			map[string]interface{}{"name": "foo", "data": []map[string]interface{}{{"a": 1}, {"b": 2}}},
			[]map[string]interface{}{{"a": 1}, {"b": 2}},
		},
		{
			"String",
			map[string]interface{}{"name": "foo", "data": "A,B,C\n1,2,3\n4,5,6", "format": "csv"},
			"A,B,C\n1,2,3\n4,5,6",
		},
	}
	for _, d := range data {
		t.Run(d.testDescription, func(t *testing.T) {
			is := is.New(t)
			r, err := New(d.descriptor)
			is.NoErr(err)
			is.True(reflect.DeepEqual(d.want, r.Data))
		})
	}
}
