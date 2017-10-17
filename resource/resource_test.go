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
		{"PathObject", map[string]interface{}{
			"path": map[string]string{"foo": "bar"},
		}},
		{"AbsolutePath", map[string]interface{}{"path": "/bar"}},
		{"InvalidRelativePath", map[string]interface{}{"path": "../bar"}},
		{"InvalidSchemeURL", map[string]interface{}{"path": "myscheme://bar"}},
		{"MixedPaths", map[string]interface{}{"path": []string{"https://bar", "bar"}}},
		{"PathAndData", map[string]interface{}{"data": "foo", "path": "foo"}},
		{"InvalidJSONStringData", map[string]interface{}{"data": "invalidJSONObjectString"}},
		{"InvalidJSONType", map[string]interface{}{"data": 1}},
	}
	for _, d := range data {
		t.Run(d.desc, func(t *testing.T) {
			is := is.New(t)
			_, err := New(d.d)
			is.True(err != nil)
		})
	}
}

func TestNew_path(t *testing.T) {
	data := []struct {
		testDescription string
		descriptor      map[string]interface{}
		want            []string
	}{
		{"URL", map[string]interface{}{"path": "http://url.com"}, []string{"http://url.com"}},
		{"FilePath", map[string]interface{}{"path": "data/foo.csv"}, []string{"data/foo.csv"}},
		{"SlicePath", map[string]interface{}{"path": []string{"https://foo.csv", "http://data/bar.csv"}}, []string{"https://foo.csv", "http://data/bar.csv"}},
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
			map[string]interface{}{"data": map[string]interface{}{"a": 1, "b": 2}},
			map[string]interface{}{"a": 1, "b": 2},
		},
		{
			"JSONArray",
			map[string]interface{}{"data": []map[string]interface{}{{"a": 1}, {"b": 2}}},
			[]map[string]interface{}{{"a": 1}, {"b": 2}},
		},
		{
			"String",
			map[string]interface{}{"data": "A,B,C\n1,2,3\n4,5,6", "format": "csv"},
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
