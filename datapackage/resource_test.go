package datapackage

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"

	"github.com/frictionlessdata/datapackage-go/validator"
	"github.com/matryer/is"
)

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
			{"SlicePath", map[string]interface{}{"name": "foo", "path": []interface{}{"https://foo.csv", "http://data/bar.csv"}}, []string{"https://foo.csv", "http://data/bar.csv"}},
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
	t.Run("DelimiterDefaultValues", func(t *testing.T) {
		is := is.New(t)
		r, err := NewResource(
			map[string]interface{}{"name": "foo", "path": "foo.csv", "dialect": map[string]interface{}{}},
			validator.MustInMemoryRegistry())
		is.NoErr(err)
		is.Equal(r.descriptor["dialect"], map[string]interface{}{"delimiter": ",", "doubleQuote": true})
	})
	t.Run("SchemaLoading", func(t *testing.T) {
		t.Run("ValidRemote", func(t *testing.T) {
			is := is.New(t)
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprintln(w, `{"fields": [{"name": "name","type": "string"}]}`)
			}))
			defer ts.Close()
			r, err := NewResource(
				map[string]interface{}{"name": "foo", "path": "foo.csv", "schema": ts.URL},
				validator.MustInMemoryRegistry(),
			)
			is.NoErr(err)
			sch, err := r.GetSchema()
			is.Equal(sch.Fields[0].Type, "string")
		})
		t.Run("InvalidRemote", func(t *testing.T) {
			_, err := NewResource(
				map[string]interface{}{"name": "foo", "path": "foo.csv", "schema": "http://foobar"},
				validator.MustInMemoryRegistry(),
			)
			if err == nil {
				t.Fatalf("want:err got:nil")
			}
		})
		t.Run("ValidLocal", func(t *testing.T) {
			is := is.New(t)
			f, err := ioutil.TempFile("", "resourceNewValidLocal")
			is.NoErr(err)
			defer os.Remove(f.Name())
			is.NoErr(ioutil.WriteFile(f.Name(), []byte(`{"fields": [{"name": "name","type": "string"}]}`), 0666))
			r, err := NewResource(
				map[string]interface{}{"name": "foo", "path": "foo.csv", "schema": f.Name()},
				validator.MustInMemoryRegistry(),
			)
			is.NoErr(err)
			sch, err := r.GetSchema()
			is.Equal(sch.Fields[0].Type, "string")
		})
		t.Run("InvalidLocal", func(t *testing.T) {
			_, err := NewResource(
				map[string]interface{}{"name": "foo", "path": "foo.csv", "schema": "foobarbez"},
				validator.MustInMemoryRegistry(),
			)
			if err == nil {
				t.Fatalf("want:err got:nil")
			}
		})
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
	r3 := NewUncheckedResource(map[string]interface{}{"path": []string{"boo.csv"}})
	is.True(r3.Tabular())
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
			"path":    "%s/data.csv",
			"profile": "tabular-data-resource",
			"schema": {"fields": [{"name": "name","type": "string"}]}
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
				"schema": {"fields": [{"name": "name", "type": "string"}]}
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
	t.Run("Dialect", func(t *testing.T) {
		t.Run("Valid", func(t *testing.T) {
			is := is.New(t)
			r, err := NewResource(
				map[string]interface{}{
					"name":    "foo",
					"data":    "name;age\n foo;42",
					"format":  "csv",
					"dialect": map[string]interface{}{"delimiter": ";", "skipInitialSpace": false, "header": true}},
				validator.MustInMemoryRegistry(),
			)
			is.NoErr(err)
			contents, err := r.ReadAll()
			is.NoErr(err)
			is.Equal(contents, [][]string{{" foo", "42"}})
		})
		t.Run("EmptyDelimiter", func(t *testing.T) {
			is := is.New(t)
			r, err := NewResource(
				map[string]interface{}{
					"name":    "foo",
					"data":    "name,age\nfoo,42",
					"format":  "csv",
					"dialect": map[string]interface{}{"delimiter": ""}},
				validator.MustInMemoryRegistry(),
			)
			is.NoErr(err)
			contents, err := r.ReadAll()
			is.NoErr(err)
			is.Equal(contents, [][]string{{"foo", "42"}})
		})
		t.Run("Multipart", func(t *testing.T) {
			is := is.New(t)
			schemaServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprintln(w, `{"fields": [{"name": "name","type": "string"}]}`)
			}))
			defer schemaServer.Close()
			res1Server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprintln(w, "name\nFoo")
			}))
			defer res1Server.Close()
			res2Server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprintln(w, "Bar")
			}))
			defer res2Server.Close()
			r, err := NewResource(
				map[string]interface{}{"name": "foo", "format": "csv", "path": []string{res1Server.URL, res2Server.URL}, "schema": schemaServer.URL},
				validator.MustInMemoryRegistry(),
			)
			is.NoErr(err)
			contents, err := r.ReadAll()
			is.NoErr(err)
			fmt.Println(contents, [][]string{{"name"}, {"Foo"}, {"Bar"}})
		})
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
