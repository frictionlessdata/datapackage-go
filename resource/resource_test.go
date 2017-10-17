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
	}
	for _, d := range data {
		t.Run(d.desc, func(t *testing.T) {
			is := is.New(t)
			_, err := New(d.d)
			is.True(err != nil)
		})
	}
}

func TestNew_urlPath(t *testing.T) {
	is := is.New(t)
	r, err := New(map[string]interface{}{"path": "http://url.com"})
	is.NoErr(err)
	is.True("http://url.com" == r.Path[0])
}

func TestNew_filePath(t *testing.T) {
	is := is.New(t)
	r, err := New(map[string]interface{}{"path": "data/foo.csv"})
	is.NoErr(err)
	is.True("data/foo.csv" == r.Path[0])
}

func TestNew_slicePath(t *testing.T) {
	is := is.New(t)
	r, err := New(map[string]interface{}{"path": []string{"data/foo.csv", "data/bar.csv"}})
	is.NoErr(err)
	is.True(reflect.DeepEqual([]string{"data/foo.csv", "data/bar.csv"}, r.Path))
}
