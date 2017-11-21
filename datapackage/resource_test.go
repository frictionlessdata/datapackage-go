package datapackage

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/frictionlessdata/datapackage-go/validator"
	"github.com/frictionlessdata/tableschema-go/csv"
	"github.com/matryer/is"
)

func ExampleResource_ReadAll() {
	descriptor := `
	{
		"name": "remote_datapackage",
		"resources": [
		  {
			"name": "example",
			"format": "csv",
			"data": "height,age,name\n180,18,Tony\n192,32,Jacob",
			"profile":"tabular-data-resource",
			"schema": {
			  "fields": [
				  {"name":"height", "type":"integer"},
				  {"name":"age", "type":"integer"},
				  {"name":"name", "type":"string"}
			  ]
			}
		  }
		]
	}
	`
	pkg, err := FromString(descriptor, validator.InMemoryLoader())
	if err != nil {
		panic(err)
	}
	res := pkg.GetResource("example")
	contents, _ := res.ReadAll(csv.LoadHeaders())
	fmt.Println(contents)
	// Output: [[180 18 Tony] [192 32 Jacob]]
}

func ExampleResource_Cast() {
	descriptor := `
	{
		"name": "remote_datapackage",
		"resources": [
		  {
			"name": "example",
			"format": "csv",
			"data": "height,age,name\n180,18,Tony\n192,32,Jacob",
			"profile":"tabular-data-resource",
			"schema": {
			  "fields": [
				  {"name":"Height", "type":"integer"},
				  {"name":"Age", "type":"integer"},
				  {"name":"Name", "type":"string"}
			  ]
			}
		  }
		]
	}
	`
	pkg, err := FromString(descriptor, validator.InMemoryLoader())
	if err != nil {
		panic(err)
	}
	res := pkg.GetResource("example")
	people := []struct {
		Height int
		Age    int
		Name   string
	}{}
	res.Cast(&people, csv.LoadHeaders())
	fmt.Printf("%+v", people)
	// Output: [{Height:180 Age:18 Name:Tony} {Height:192 Age:32 Name:Jacob}]
}

func ExampleNewResourceWithDefaultRegistry() {
	res, _ := NewResourceWithDefaultRegistry(r1)
	fmt.Println(res.Name())
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
				is.True(r.name == d.want)
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
				is.True(reflect.DeepEqual(d.want, r.path))
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
				if !reflect.DeepEqual(reflect.ValueOf(d.want).Interface(), r.data) {
					t.Fatalf("want:%v type:%v got:%v type:%v", d.want, reflect.TypeOf(d.want), r.data, reflect.TypeOf(r.data))
				}
			})
		}
	})
}

func TestResource_Descriptor(t *testing.T) {
	is := is.New(t)
	r, err := NewResource(r1, validator.MustInMemoryRegistry())
	is.NoErr(err)

	cpy := r.Descriptor()
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
		is.Equal(r.Descriptor(), r2Filled)
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
	r := NewUncheckedResource(map[string]interface{}{"profile": "tabular-data-resource"})
	is.True(r.Tabular())
	r1 := NewUncheckedResource(map[string]interface{}{"profile": "data-resource"})
	is.True(!r1.Tabular())
	r2 := NewUncheckedResource(map[string]interface{}{"format": "csv"})
	is.True(r2.Tabular())
}

func TestResource_ReadAll(t *testing.T) {
	t.Run("LoadData", func(t *testing.T) {
		is := is.New(t)
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintln(w, "name\nfoo")
		}))
		defer ts.Close()
		resStr := fmt.Sprintf(`
		{
			"name":    "names",
			"path":    "%s",
			"profile": "tabular-data-resource",
			"schema": {
				"fields": [
				{
					"name": "name",
					"type": "string"
				}
				]
			}
		}`, ts.URL)
		res, err := NewResourceFromString(resStr, validator.MustInMemoryRegistry())
		is.NoErr(err)
		contents, err := res.ReadAll()
		is.NoErr(err)
		is.Equal(contents, [][]string{{"name"}, {"foo"}})
	})
	t.Run("InlineData", func(t *testing.T) {
		is := is.New(t)
		resStr := `
			{
				"name":    "names",
				"data":    "name\nfoo",
				"format":  "csv",
				"profile": "tabular-data-resource",
				"schema": {
					"fields": [{"name": "name", "type": "string"}]
				}
			}`
		res, err := NewResourceFromString(resStr, validator.MustInMemoryRegistry())
		is.NoErr(err)
		contents, err := res.ReadAll()
		is.NoErr(err)
		is.Equal(contents, [][]string{{"name"}, {"foo"}})
	})
	t.Run("InvalidProfileType", func(t *testing.T) {
		r1 := NewUncheckedResource(map[string]interface{}{"profile": "data-resource"})
		_, err := r1.ReadAll()
		if err == nil {
			t.Fatalf("want:nil got:err")
		}
	})
}

func TestResource_Iter(t *testing.T) {
	is := is.New(t)
	resStr := `
		{
			"name":    "iter",
			"data":    "name",
			"format":  "csv",
			"profile": "tabular-data-resource",
			"schema": {"fields": [{"name": "foo", "type": "string"}]}
		}`
	res, err := NewResourceFromString(resStr, validator.MustInMemoryRegistry())
	is.NoErr(err)
	iter, err := res.Iter()
	is.NoErr(err)
	is.True(iter.Next())
	is.Equal(iter.Row(), []string{"name"})
	is.True(!iter.Next())
}

func TestResource_GetSchema(t *testing.T) {
	t.Run("Valid", func(t *testing.T) {
		is := is.New(t)
		resStr := `
			{
				"name":    "iter",
				"data":    "32",
				"format":  "csv",
				"profile": "tabular-data-resource",
				"schema": {"fields": [{"name": "Age", "type": "integer"}]}
			}`
		res, err := NewResourceFromString(resStr, validator.MustInMemoryRegistry())
		is.NoErr(err)
		sch, err := res.GetSchema()
		is.NoErr(err)
		row := struct {
			Age int
		}{}
		sch.CastRow([]string{"32"}, &row)
		is.Equal(row.Age, 32)
	})
	t.Run("NoSchema", func(t *testing.T) {
		res := NewUncheckedResource(map[string]interface{}{})
		_, err := res.GetSchema()
		if err == nil {
			t.Fatal("want:err got:nil")
		}
	})
}

func TestResource_Cast(t *testing.T) {
	resStr := `
	{
		"name":    "iter",
		"data":    "32",
		"format":  "csv",
		"profile": "tabular-data-resource",
		"schema": {"fields": [{"name": "Age", "type": "integer"}]}
	}`
	rows := []struct {
		Age int
	}{}
	t.Run("Valid", func(t *testing.T) {
		is := is.New(t)
		res, err := NewResourceFromString(resStr, validator.MustInMemoryRegistry())
		is.NoErr(err)
		is.NoErr(res.Cast(&rows))
		is.Equal(rows[0].Age, 32)
	})
	t.Run("NoSchema", func(t *testing.T) {
		res := NewUncheckedResource(map[string]interface{}{})
		if res.Cast(&rows) == nil {
			t.Fatal("want:err got:nil")
		}
	})
	t.Run("NoData", func(t *testing.T) {
		res := NewUncheckedResource(map[string]interface{}{
			"schema": map[string]interface{}{},
		})
		if res.Cast(&rows) == nil {
			t.Fatal("want:err got:nil")
		}
	})
}
