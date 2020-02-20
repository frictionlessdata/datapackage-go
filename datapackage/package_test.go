package datapackage

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/frictionlessdata/datapackage-go/validator"
	"github.com/matryer/is"
)

var invalidResource = map[string]interface{}{"name": "res1"}
var r1 = map[string]interface{}{"name": "res1", "path": "foo.csv"}
var r1Filled = map[string]interface{}{"name": "res1", "path": "foo.csv", "profile": "data-resource", "encoding": "utf-8"}
var r1Str = `{
  "profile": "data-package",
  "resources": [
    {
      "encoding": "utf-8",
      "name": "res1",
      "path": "foo.csv",
      "profile": "data-resource"
    }
  ]
}`
var r2 = map[string]interface{}{"name": "res2", "path": "bar.csv"}
var r2Filled = map[string]interface{}{"name": "res2", "path": "bar.csv", "profile": "data-resource", "encoding": "utf-8"}

func ExampleLoad_readAll() {
	dir, _ := ioutil.TempDir("", "datapackage_exampleload")
	defer os.RemoveAll(dir)
	descriptorPath := filepath.Join(dir, "pkg.json")
	descriptorContents := `{"resources": [{ 
		  "name": "res1",
		  "path": "data.csv",
		  "profile": "tabular-data-resource",
		  "schema": {"fields": [{"name":"name", "type":"string"}]}
		}]}`
	ioutil.WriteFile(descriptorPath, []byte(descriptorContents), 0666)

	resPath := filepath.Join(dir, "data.csv")
	resContent := []byte("foo\nbar")
	ioutil.WriteFile(resPath, resContent, 0666)

	pkg, _ := Load(descriptorPath, validator.InMemoryLoader())
	contents, _ := pkg.GetResource("res1").ReadAll()
	fmt.Println(contents)
	// Output: [[foo] [bar]]
}

func ExampleLoad_readRaw() {
	dir, _ := ioutil.TempDir("", "datapackage_exampleload")
	defer os.RemoveAll(dir)
	descriptorPath := filepath.Join(dir, "pkg.json")
	descriptorContents := `{"resources": [{ 
		  "name": "res1",
		  "path": "schemaorg.json",
		  "format": "application/ld+json",
		  "profile": "data-resource"
		}]}`
	ioutil.WriteFile(descriptorPath, []byte(descriptorContents), 0666)

	resPath := filepath.Join(dir, "schemaorg.json")
	resContent := []byte(`{"@context": {"@vocab": "http://schema.org/"}}`)
	ioutil.WriteFile(resPath, resContent, 0666)

	pkg, _ := Load(descriptorPath, validator.InMemoryLoader())
	rc, _ := pkg.GetResource("res1").RawRead()
	defer rc.Close()
	contents, _ := ioutil.ReadAll(rc)
	fmt.Println(string(contents))
	// Output: {"@context": {"@vocab": "http://schema.org/"}}
}

func ExampleLoad_readAllRemote() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// If the request is for data, returns the content.
		switch {
		case r.RequestURI == "/data.csv":
			fmt.Fprintf(w, "foo\nbar")
		default:
			fmt.Fprintf(w, `{"resources": [{ 
				"name": "res1",
				"path": "data.csv",
				"profile": "tabular-data-resource",
				"schema": {"fields": [{"name":"name", "type":"string"}]}
			  }]}`)
		}
	}))
	defer ts.Close()
	pkg, _ := Load(ts.URL, validator.InMemoryLoader())
	contents, _ := pkg.GetResource("res1").ReadAll()
	fmt.Println(contents)
	// Output: [[foo] [bar]]
}

func ExampleLoad_cast() {
	dir, _ := ioutil.TempDir("", "datapackage_exampleload")
	defer os.RemoveAll(dir)
	descriptorPath := filepath.Join(dir, "pkg.json")
	descriptorContents := `{"resources": [{ 
		  "name": "res1",
		  "path": "data.csv",
		  "profile": "tabular-data-resource",
		  "schema": {"fields": [{"name":"name", "type":"string"}]}
		}]}`
	ioutil.WriteFile(descriptorPath, []byte(descriptorContents), 0666)

	resPath := filepath.Join(dir, "data.csv")
	resContent := []byte("foo\nbar")
	ioutil.WriteFile(resPath, resContent, 0666)

	pkg, _ := Load(descriptorPath, validator.InMemoryLoader())
	res := pkg.GetResource("res1")
	people := []struct {
		Name string `tableheader:"name"`
	}{}
	res.Cast(&people)
	fmt.Printf("%+v", people)
	// Output: [{Name:foo} {Name:bar}]
}

func TestPackage_GetResource(t *testing.T) {
	is := is.New(t)
	pkg, err := New(map[string]interface{}{"resources": []interface{}{r1}}, ".", validator.InMemoryLoader())
	is.NoErr(err)
	is.Equal(pkg.GetResource("res1").name, "res1")
	is.True(pkg.GetResource("foooooo") == nil)
}

func TestPackage_AddResource(t *testing.T) {
	t.Run("ValidDescriptor", func(t *testing.T) {
		is := is.New(t)
		pkg, _ := New(map[string]interface{}{"resources": []interface{}{r1}}, ".", validator.InMemoryLoader())
		is.NoErr(pkg.AddResource(r2))

		// Checking resources.
		is.Equal(len(pkg.resources), 2)
		is.Equal(pkg.resources[0].name, "res1")
		is.Equal(pkg.resources[1].name, "res2")

		// Checking descriptor.
		resDesc := pkg.descriptor["resources"].([]interface{})
		is.Equal(len(resDesc), 2)
		is.Equal(resDesc[0], r1Filled)
		is.Equal(resDesc[1], r2Filled)
	})
	t.Run("InvalidResource", func(t *testing.T) {
		pkg, _ := New(map[string]interface{}{"resources": []interface{}{r1}}, ".", validator.InMemoryLoader())
		if err := pkg.AddResource(invalidResource); err == nil {
			t.Fatalf("want:err got:nil")
		}
	})
}

func TestPackage_RemoveResource(t *testing.T) {
	t.Run("Existing", func(t *testing.T) {
		is := is.New(t)
		pkg, _ := New(map[string]interface{}{"resources": []interface{}{r1, r2}}, ".", validator.InMemoryLoader())
		pkg.RemoveResource("res1")

		resDesc := pkg.descriptor["resources"].([]interface{})
		is.Equal(len(resDesc), 1)
		is.Equal(resDesc[0], r2Filled)
		is.Equal(len(pkg.resources), 1)
		is.Equal(pkg.resources[0].name, "res2")
	})
	t.Run("NonExisting", func(t *testing.T) {
		is := is.New(t)
		pkg, _ := New(map[string]interface{}{"resources": []interface{}{r1}}, ".", validator.InMemoryLoader())
		pkg.RemoveResource("invalid")

		resDesc := pkg.descriptor["resources"].([]interface{})
		is.Equal(len(resDesc), 1)
		is.Equal(resDesc[0], r1Filled)
		is.Equal(len(pkg.resources), 1)
		is.Equal(pkg.resources[0].name, "res1")
	})
}

func TestPackage_ResourceNames(t *testing.T) {
	is := is.New(t)
	pkg, _ := New(map[string]interface{}{"resources": []interface{}{r1, r2}}, ".", validator.InMemoryLoader())
	is.Equal(pkg.ResourceNames(), []string{"res1", "res2"})
}

func TestPackage_Resources(t *testing.T) {
	is := is.New(t)
	pkg, _ := New(map[string]interface{}{"resources": []interface{}{r1, r2}}, ".", validator.InMemoryLoader())
	resources := pkg.Resources()
	is.Equal(resources[0].name, "res1")
	is.Equal(resources[1].name, "res2")

	// Changing the returned slice must not change the package.
	resources = append(resources, &Resource{name: "foo"})
	is.Equal(len(pkg.ResourceNames()), 2)
}

func TestPackage_Descriptor(t *testing.T) {
	is := is.New(t)
	pkg, _ := New(map[string]interface{}{"resources": []interface{}{r1}}, ".", validator.InMemoryLoader())
	cpy := pkg.Descriptor()
	is.Equal(pkg.descriptor, cpy)
}

func TestPackage_Update(t *testing.T) {
	t.Run("ValidResource", func(t *testing.T) {
		is := is.New(t)
		pkg, _ := New(map[string]interface{}{"resources": []interface{}{r1}}, ".", validator.InMemoryLoader())
		newDesc := map[string]interface{}{"resources": []interface{}{r2}}
		is.NoErr(pkg.Update(newDesc, validator.InMemoryLoader()))
		is.Equal(pkg.Descriptor(), map[string]interface{}{"profile": "data-package", "resources": []interface{}{r2Filled}})
	})
	t.Run("InvalidResource", func(t *testing.T) {
		pkg, _ := New(map[string]interface{}{"resources": []interface{}{r1}}, ".", validator.InMemoryLoader())
		newDesc := map[string]interface{}{"resources": []interface{}{invalidResource}}
		if err := pkg.Update(newDesc, validator.InMemoryLoader()); err == nil {
			t.Fatalf("want:err got:nil")
		}
	})
}

func TestFromDescriptor(t *testing.T) {
	t.Run("ValidationErrors", func(t *testing.T) {
		data := []struct {
			desc       string
			descriptor map[string]interface{}
		}{
			{"EmptyMap", map[string]interface{}{}},
			{"InvalidResourcePropertyType", map[string]interface{}{"resources": 10}},
			{"InvalidResource", map[string]interface{}{"resources": []interface{}{map[string]interface{}{}}}},
			{"InvalidResourceType", map[string]interface{}{"resources": []interface{}{1}}},
			{"ProfileNotAString", map[string]interface{}{"profile": 1, "resources": []interface{}{r1}}},
			{"ErrorCloning", map[string]interface{}{"profile": [][][]string{}, "resources": []interface{}{r1}}},
		}
		for _, d := range data {
			t.Run(d.desc, func(t *testing.T) {
				if _, err := New(d.descriptor, ".", validator.InMemoryLoader()); err == nil {
					t.Fatalf("want:err got:nil")
				}
			})
		}
	})
	t.Run("ValidDescriptor", func(t *testing.T) {
		is := is.New(t)
		pkg, err := New(map[string]interface{}{"resources": []interface{}{r1}}, ".", validator.InMemoryLoader())
		is.NoErr(err)
		is.Equal(len(pkg.resources), 1)
		is.Equal(pkg.resources[0].name, "res1")
		resources := pkg.descriptor["resources"].([]interface{})
		is.Equal(len(resources), 1)
		is.Equal(resources[0], r1Filled)
	})
}

func TestPackage_SaveDescriptor(t *testing.T) {
	t.Run("Valid", func(t *testing.T) {
		is := is.New(t)

		// Creating temporary empty directory and making sure we remove it.
		dir, err := ioutil.TempDir("", "datapackage_save")
		is.NoErr(err)
		defer os.RemoveAll(dir)
		fName := filepath.Join(dir, "pkg.json")

		// Saving package descriptor.
		pkg, _ := New(map[string]interface{}{"resources": []interface{}{r1}}, ".", validator.InMemoryLoader())
		is.NoErr(pkg.SaveDescriptor(fName))

		// Checking descriptor contents.
		buf, err := ioutil.ReadFile(fName)
		is.NoErr(err)
		is.Equal(string(buf), r1Str)
	})
}

func TestPackage_Zip(t *testing.T) {
	t.Run("Valid", func(t *testing.T) {
		is := is.New(t)

		// Creating temporary empty file and making sure we remove it.
		dir, err := ioutil.TempDir("", "datapackage_testzip")
		is.NoErr(err)
		defer os.RemoveAll(dir)
		fName := filepath.Join(dir, "pkg.zip")

		// Creating contents and zipping package.
		descriptorContents := `{"resources": [{
					"name": "res1",
					"path": "data.csv",
					"profile": "tabular-data-resource",
					"schema": {"fields": [{"name":"name", "type":"string"}]}
				  }]}`
		pkg, _ := FromString(descriptorContents, dir, validator.InMemoryLoader())
		fmt.Println(pkg.Descriptor())

		resPath := filepath.Join(dir, "data.csv")
		resContents := []byte("foo\nbar")
		ioutil.WriteFile(resPath, resContents, 0666)
		is.NoErr(pkg.Zip(fName))

		// Checking zip contents.
		reader, err := zip.OpenReader(fName)
		is.NoErr(err)
		defer reader.Close()
		is.Equal(2, len(reader.File))

		var buf bytes.Buffer
		descriptor, err := reader.File[0].Open()
		is.NoErr(err)
		defer descriptor.Close()
		io.Copy(&buf, descriptor)

		filledDescriptor := `{
  "profile": "data-package",
  "resources": [
    {
      "encoding": "utf-8",
      "name": "res1",
      "path": "data.csv",
      "profile": "tabular-data-resource",
      "schema": {
        "fields": [
          {
            "name": "name",
            "type": "string"
          }
        ]
      }
    }
  ]
}`
		is.Equal(buf.String(), filledDescriptor)

		buf.Reset()
		data, err := reader.File[1].Open()
		is.NoErr(err)
		defer data.Close()
		io.Copy(&buf, data)
		is.Equal(buf.String(), string(resContents))
	})
	t.Run("ValidDataInSubdir", func(t *testing.T) {
		is := is.New(t)

		// Creating temporary empty directory and making sure we remove it.
		dir, err := ioutil.TempDir("", "datapackage_testzip")
		is.NoErr(err)
		defer os.Remove(dir)

		dataDir := filepath.Join(dir, "data")
		is.NoErr(os.Mkdir(dataDir, os.ModePerm))
		resPath := filepath.Join(dataDir, "data.csv")
		resContents := []byte("foo\nbar")
		is.NoErr(ioutil.WriteFile(resPath, resContents, os.ModePerm))

		// Creating contents and zipping package.
		d := map[string]interface{}{
			"resources": []interface{}{
				map[string]interface{}{
					"name":   "res1",
					"path":   "./data/data.csv",
					"format": "csv",
				},
			},
		}
		pkg, _ := New(d, dir, validator.InMemoryLoader())
		fmt.Println(pkg.Descriptor())

		fName := filepath.Join(dir, "pkg.zip")
		is.NoErr(pkg.Zip(fName))

		// Checking zip contents.
		reader, err := zip.OpenReader(fName)
		is.NoErr(err)
		defer reader.Close()
		is.Equal(2, len(reader.File))

		var buf bytes.Buffer
		readDescriptor, err := reader.File[0].Open()
		is.NoErr(err)
		defer readDescriptor.Close()
		io.Copy(&buf, readDescriptor)

		filledDescriptor := `{
  "profile": "data-package",
  "resources": [
    {
      "encoding": "utf-8",
      "format": "csv",
      "name": "res1",
      "path": "./data/data.csv",
      "profile": "data-resource"
    }
  ]
}`
		is.Equal(buf.String(), filledDescriptor)

		is.Equal("data/data.csv", reader.File[1].Name)
		data, err := reader.File[1].Open()
		is.NoErr(err)
		defer data.Close()
		buf.Reset()
		io.Copy(&buf, data)
		is.Equal(buf.String(), string(resContents))
	})
}
func TestFromReader(t *testing.T) {
	t.Run("ValidJSON", func(t *testing.T) {
		is := is.New(t)
		_, err := FromReader(strings.NewReader(`{"resources":[{"name":"res", "path":"foo.csv"}]}`), ".", validator.InMemoryLoader())
		is.NoErr(err)
	})
	t.Run("InvalidJSON", func(t *testing.T) {
		is := is.New(t)
		_, err := FromReader(strings.NewReader(`{resources}`), ".", validator.InMemoryLoader())
		is.True(err != nil)
	})
}

func TestLoad(t *testing.T) {
	is := is.New(t)
	// Creating temporary empty directory and making sure we remove it.
	dir, err := ioutil.TempDir("", "datapackage_load")
	is.NoErr(err)
	defer os.RemoveAll(dir)

	t.Run("Local", func(t *testing.T) {
		is := is.New(t)
		fName := filepath.Join(dir, "pkg.json")
		is.NoErr(ioutil.WriteFile(fName, []byte(r1Str), 0666))
		defer os.Remove(fName)

		pkg, err := Load(fName, validator.InMemoryLoader())
		is.NoErr(err)
		res := pkg.GetResource("res1")
		is.Equal(res.name, "res1")
		is.Equal(res.path, []string{"foo.csv"})
	})
	t.Run("LocalZip", func(t *testing.T) {
		is := is.New(t)
		// Creating a zip file.
		fName := filepath.Join(dir, "pkg.zip")
		zipFile, err := os.Create(fName)
		is.NoErr(err)
		defer zipFile.Close()

		// Adding a datapackage.json file to the zip with proper contents.
		w := zip.NewWriter(zipFile)
		f, err := w.Create("datapackage.json")
		is.NoErr(err)
		_, err = f.Write([]byte(r1Str))
		is.NoErr(err)
		is.NoErr(w.Close())

		// Load and check package.
		pkg, err := Load(fName, validator.InMemoryLoader())
		is.NoErr(err)
		res := pkg.GetResource("res1")
		is.Equal(res.name, "res1")
		is.Equal(res.path, []string{"foo.csv"})
	})
	t.Run("LocalZipWithSubdirs", func(t *testing.T) {
		is := is.New(t)
		// Creating a zip file.
		fName := filepath.Join(dir, "pkg.zip")
		zipFile, err := os.Create(fName)
		is.NoErr(err)
		defer zipFile.Close()

		// Adding a datapackage.json file to the zip with proper contents.
		w := zip.NewWriter(zipFile)
		f, err := w.Create("datapackage.json")
		is.NoErr(err)
		_, err = f.Write([]byte(`{
			"profile": "data-package",
			"resources": [
			  {
				"encoding": "utf-8",
				"name": "res1",
				"path": "data/foo.csv",
				"profile": "data-resource"
			  }
			]
		  }`))
		is.NoErr(err)
		// Writing a file which is in a subdir.
		f1, err := w.Create("data/foo.csv")
		is.NoErr(err)
		_, err = f1.Write([]byte(`foo`))
		is.NoErr(err)
		is.NoErr(w.Close())

		// Load and check package.
		pkg, err := Load(fName, validator.InMemoryLoader())
		is.NoErr(err)
		res := pkg.GetResource("res1")
		is.Equal(res.name, "res1")
		is.Equal(res.path, []string{"data/foo.csv"})
		contents, err := res.ReadAll()
		is.NoErr(err)
		is.Equal(contents[0], []string{"foo"})
	})
	t.Run("Remote", func(t *testing.T) {
		is := is.New(t)
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintln(w, r1Str)
		}))
		defer ts.Close()
		data := []struct {
			desc       string
			pathSuffix string
		}{
			{"Empty Path", ""},
			{"Non-EmptyPath", "/datapackage.json"},
			{"EndsInSlash", "/"},
		}
		for _, d := range data {
			t.Run(d.desc, func(t *testing.T) {
				is := is.New(t)
				pkg, err := Load(ts.URL+d.pathSuffix, validator.InMemoryLoader())
				is.NoErr(err)
				res := pkg.GetResource("res1")
				is.Equal(res.name, "res1")
				is.Equal(res.path, []string{"foo.csv"})
				is.Equal(res.basePath, ts.URL+"/")
			})
		}
	})
	t.Run("InvalidPath", func(t *testing.T) {
		_, err := Load("foobar", validator.InMemoryLoader())
		if err == nil {
			t.Fatalf("want:err got:nil")
		}
	})
	t.Run("InvalidZipFile", func(t *testing.T) {
		is := is.New(t)
		// Creating an empty zip file.
		fName := filepath.Join(dir, "pkg.zip")
		zipFile, err := os.Create(fName)
		is.NoErr(err)
		defer zipFile.Close()
		// Asserting error.
		_, err = Load(fName, validator.InMemoryLoader())
		if err == nil {
			t.Fatalf("want:err got:nil")
		}
	})
	t.Run("InvalidZipFileNameInContent", func(t *testing.T) {
		is := is.New(t)
		// Creating a zip file.
		fName := filepath.Join(dir, "pkg.zip")
		zipFile, err := os.Create(fName)
		is.NoErr(err)
		defer zipFile.Close()

		// Adding a file to the zip with proper contents.
		w := zip.NewWriter(zipFile)
		f, err := w.Create("otherpackage.json")
		is.NoErr(err)
		_, err = f.Write([]byte(r1Str))
		is.NoErr(err)
		is.NoErr(w.Close())

		// Asserting error.
		_, err = Load(fName, validator.InMemoryLoader())
		if err == nil {
			t.Fatalf("want:err got:nil")
		}
	})
}

func TestLoadPackageSchemas(t *testing.T) {
	is := is.New(t)
	schStr := `{"fields": [{"name":"name", "type":"string"}]}`
	schMap := map[string]interface{}{"fields": []interface{}{map[string]interface{}{"name": "name", "type": "string"}}}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, schStr)
	}))
	defer ts.Close()
	in := fmt.Sprintf(`{
		"schema": "%s",
		"resources": [{ 
		"name": "res1",
		"path": "data.csv",
		"profile": "tabular-data-resource",
		"schema": "%s"
	  }]}`, ts.URL, ts.URL)
	pkg, err := FromString(in, ".", validator.InMemoryLoader())
	is.NoErr(err)
	is.Equal(pkg.Descriptor()["schema"], schMap)
	is.Equal(pkg.GetResource("res1").Descriptor()["schema"], schMap)
}
