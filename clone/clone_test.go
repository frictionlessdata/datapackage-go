package clone

import (
	"testing"

	"github.com/matryer/is"
)

func TestDescriptor(t *testing.T) {
	is := is.New(t)
	d := map[string]interface{}{
		"name": "pkg1",
		"boo":  1,
		"resources": []interface{}{
			map[string]interface{}{"name": "res1"}, map[string]interface{}{"name": "res2"},
		},
	}
	cpy, err := Descriptor(d)
	is.NoErr(err)
	is.Equal(d, cpy)

	// Error: Unregistered gob type: map[int]interface{}.
	_, err = Descriptor(map[string]interface{}{"boo": map[int]interface{}{}})
	if err == nil {
		t.Fatal("want:err got:nil")
	}
}
