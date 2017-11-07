package pkg

import (
	"fmt"
	"strings"
	"testing"

	"github.com/frictionlessdata/datapackage-go/resource"
	"github.com/matryer/is"
)

func validResource(name string) resourceFactory {
	return func(map[string]interface{}) (*resource.Resource, error) { return &resource.Resource{Name: name}, nil }
}

var invalidResource = func(map[string]interface{}) (*resource.Resource, error) { return nil, fmt.Errorf("") }

func TestPkg_GetResource(t *testing.T) {
	is := is.New(t)
	in := `{"resources":[{"name":"res"}]}`
	p, err := fromReader(strings.NewReader(in), validResource("res"))
	is.NoErr(err)
	is.Equal("res", p.GetResource("res").Name)
}

func TestFromDescriptor(t *testing.T) {
	t.Run("ValidationErrors", func(t *testing.T) {
		is := is.New(t)
		data := []struct {
			desc       string
			descriptor map[string]interface{}
			resFactory resourceFactory
		}{
			{"EmptyMap", map[string]interface{}{}, validResource("res")},
			{"InvalidResourcePropertyType", map[string]interface{}{
				"resources": 10,
			}, validResource("res")},
			{"EmptyResourcesSlice", map[string]interface{}{
				"resources": []interface{}{},
			}, validResource("res")},
			{"InvalidResource", map[string]interface{}{
				"resources": []interface{}{map[string]interface{}{}},
			}, invalidResource},
			{"InvalidResourceType", map[string]interface{}{
				"resources": []interface{}{1},
			}, validResource("res")},
		}
		for _, d := range data {
			_, err := fromDescriptor(d.descriptor, d.resFactory)
			is.True(err != nil)
		}
	})
	t.Run("ValidDescriptor", func(t *testing.T) {
		is := is.New(t)
		_, err := fromDescriptor(
			map[string]interface{}{
				"resources": []interface{}{map[string]interface{}{}},
			},
			validResource("res"),
		)
		is.NoErr(err)
	})
}

func TestFromReader(t *testing.T) {
	t.Run("ValidJSON", func(t *testing.T) {
		is := is.New(t)
		_, err := fromReader(strings.NewReader(`{"resources":[{"name":"res"}]}`), validResource("res"))
		is.NoErr(err)
	})
	t.Run("InvalidJSON", func(t *testing.T) {
		is := is.New(t)
		_, err := fromReader(strings.NewReader(`{resources}`), validResource("res"))
		is.True(err != nil)
	})
}
