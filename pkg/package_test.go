package pkg

import (
	"fmt"
	"strings"
	"testing"

	"github.com/matryer/is"
)

var invalidResource = func(map[string]interface{}) (*Resource, error) { return nil, fmt.Errorf("") }
var r1 = map[string]interface{}{"name": "res1"}
var r2 = map[string]interface{}{"name": "res2"}

type fakeValidator struct {
	jsonSchemaValidator
	valid bool
}

func (v *fakeValidator) IsValid(_ map[string]interface{}) bool {
	return len(v.jsonSchemaValidator.errors) == 0
}

func newFakeValidator(_ string) (descriptorValidator, error) {
	return &fakeValidator{}, nil
}

func TestPackage_GetResource(t *testing.T) {
	is := is.New(t)
	p := Package{descriptor: map[string]interface{}{"resources": []interface{}{map[string]interface{}{"name": "res"}}}}
	r, err := buildResources(p.descriptor["resources"], NewUncheckedResource)
	is.NoErr(err)
	p.resources = r
	is.Equal(p.GetResource("res").Name, "res")
	is.True(p.GetResource("foooooo") == nil)
}

func TestPackage_AddResource(t *testing.T) {
	t.Run("ValidDescriptor", func(t *testing.T) {
		is := is.New(t)

		p := Package{descriptor: map[string]interface{}{"resources": []interface{}{}}, resFactory: NewUncheckedResource}
		is.NoErr(p.AddResource(r1))
		is.NoErr(p.AddResource(r2))
		is.Equal(len(p.resources), 2)
		is.Equal(p.resources[0].Name, "res1")
		is.Equal(p.resources[1].Name, "res2")
		resources := p.descriptor["resources"].([]interface{})
		is.Equal(len(resources), 2)
		is.Equal(resources[0], r1)
		is.Equal(resources[1], r2)
	})
	t.Run("CodedPackage", func(t *testing.T) {
		is := is.New(t)
		p := Package{descriptor: map[string]interface{}{"resources": []interface{}{}}, resFactory: NewUncheckedResource}
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
		p := Package{descriptor: map[string]interface{}{"resources": []interface{}{}}, resFactory: NewUncheckedResource}
		is.NoErr(p.AddResource(r1))
		is.NoErr(p.AddResource(r2))
		p.RemoveResource("res1")
		is.Equal(len(p.descriptor), 1)
		is.Equal(len(p.resources), 1)
		desc0, err := p.resources[0].Descriptor()
		is.NoErr(err)
		is.Equal(p.descriptor["resources"].([]interface{})[0], desc0)

		// Remove a non-existing resource and checks if everything stays the same.
		p.RemoveResource("res1")
		is.Equal(len(p.descriptor), 1)
		is.Equal(len(p.resources), 1)
		is.Equal(p.descriptor["resources"].([]interface{})[0], desc0)
	})
}

func TestPackage_ResourceNames(t *testing.T) {
	is := is.New(t)
	p := Package{descriptor: map[string]interface{}{"resources": []interface{}{}}, resFactory: NewUncheckedResource}
	is.NoErr(p.AddResource(r1))
	is.NoErr(p.AddResource(r2))
	is.Equal(p.ResourceNames(), []string{"res1", "res2"})
}

func TestPackage_Descriptor(t *testing.T) {
	is := is.New(t)
	p := Package{descriptor: map[string]interface{}{"resources": []interface{}{}}, resFactory: NewUncheckedResource}
	is.NoErr(p.AddResource(r1))
	c, err := p.Descriptor()
	is.NoErr(err)
	is.Equal(p.descriptor, c)
}

func TestPackage_Update(t *testing.T) {
	is := is.New(t)
	p, err := fromDescriptor(
		map[string]interface{}{"resources": []interface{}{
			map[string]interface{}{"name": "res1"},
		}},
		NewUncheckedResource,
		newFakeValidator)
	is.NoErr(err)

	newDesc := map[string]interface{}{"resources": []interface{}{
		map[string]interface{}{"name": "res1"},
		map[string]interface{}{"name": "res2"},
	}}
	is.NoErr(p.Update(newDesc))
	d, err := p.Descriptor()
	is.NoErr(err)
	is.Equal(d, newDesc)

	// Invalid resource.
	p.resFactory = invalidResource
	is.True(p.Update(newDesc) != nil)
}

func TestFromDescriptor(t *testing.T) {
	t.Run("ValidationErrors", func(t *testing.T) {
		data := []struct {
			desc       string
			descriptor map[string]interface{}
			resFactory resourceFactory
			valFactory validatorFactory
		}{
			{"EmptyMap", map[string]interface{}{}, NewUncheckedResource, newFakeValidator},
			{"InvalidResourcePropertyType", map[string]interface{}{
				"resources": 10,
			}, NewUncheckedResource, newFakeValidator},
			{"InvalidResource", map[string]interface{}{
				"resources": []interface{}{map[string]interface{}{}},
			}, invalidResource, newFakeValidator},
			{"InvalidResourceType", map[string]interface{}{
				"resources": []interface{}{1},
			}, NewUncheckedResource, newFakeValidator},
		}
		for _, d := range data {
			t.Run(d.desc, func(t *testing.T) {
				is := is.New(t)
				_, err := fromDescriptor(d.descriptor, d.resFactory, d.valFactory)
				is.True(err != nil)
			})
		}
	})
	t.Run("ValidDescriptor", func(t *testing.T) {
		is := is.New(t)
		r1 := map[string]interface{}{"name": "res"}
		p, err := fromDescriptor(
			map[string]interface{}{"resources": []interface{}{r1}},
			NewUncheckedResource, newFakeValidator,
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
		_, err := fromReader(strings.NewReader(`{"resources":[{"name":"res"}]}`), NewUncheckedResource, newFakeValidator)
		is.NoErr(err)
	})
	t.Run("InvalidJSON", func(t *testing.T) {
		is := is.New(t)
		_, err := fromReader(strings.NewReader(`{resources}`), NewUncheckedResource, newFakeValidator)
		is.True(err != nil)
	})
}

func TestValid(t *testing.T) {
	is := is.New(t)
	is.NoErr(validateDescriptor(map[string]interface{}{"profile": "boo"}, newFakeValidator))
	if validateDescriptor(map[string]interface{}{}, newFakeValidator) == nil {
		t.Fatalf("want:err got:nil")
	}
}
