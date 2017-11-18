package pkg

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xeipuuv/gojsonschema"

	"github.com/matryer/is"
)

const dontUseLocalSchemaFiles = false
const simpleSchema = `{
	"$schema": "http://json-schema.org/draft-04/schema#",
	"type": "object",
	"oneOf": [{"required": ["name"]}]
}`

func TestProfileIsValid(t *testing.T) {
	// Avoiding touch the disk.
	useLocalSchemaFiles = false
	defer func() { useLocalSchemaFiles = true }()
	t.Run("ValidProfile", func(t *testing.T) {
		is := is.New(t)
		v, err := newProfileValidator("data-package")
		is.NoErr(err)
		is.True(v.IsValid(map[string]interface{}{"resources": []interface{}{map[string]interface{}{"name": "res1", "path": "foo.csv"}}}))
	})
	t.Run("InvalidProfile", func(t *testing.T) {
		is := is.New(t)
		v, err := newProfileValidator("data-package")
		is.NoErr(err)
		is.True(!v.IsValid(map[string]interface{}{"resources": []interface{}{map[string]interface{}{"name": "res1"}}}))
	})
}

func TestLoadSchema(t *testing.T) {
	t.Run("ThirdPartyRemoteSchema", func(t *testing.T) {
		is := is.New(t)
		ts := serverForTests(simpleSchema)
		defer ts.Close()
		s, err := loadSchema(map[string]profileSpec{}, ts.URL, dontUseLocalSchemaFiles)
		is.NoErr(err)
		_, err = s.Validate(gojsonschema.NewGoLoader(map[string]interface{}{"name": "foo"}))
		is.NoErr(err)
	})
	t.Run("DataPackageSchemaRegistry", func(t *testing.T) {
		is := is.New(t)
		ts := serverForTests(simpleSchema)
		defer ts.Close()
		fmt.Println(ts.URL)
		reg := map[string]profileSpec{"schemaID": profileSpec{ID: "schemaID", Schema: ts.URL}}
		s, err := loadSchema(reg, "schemaID", dontUseLocalSchemaFiles)
		is.NoErr(err)
		_, err = s.Validate(gojsonschema.NewGoLoader(map[string]interface{}{"name": "foo"}))
		is.NoErr(err)
	})
	t.Run("LocalRegistry", func(t *testing.T) {
		is := is.New(t)
		schemas := []string{
			"data-package.json",
			"data-resource.json",
			"fiscal-data-package.json",
			"table-schema.json",
			"tabular-data-package.json",
			"tabular-data-resource.json",
		}
		for _, s := range schemas {
			reg := map[string]profileSpec{"schemaID": profileSpec{ID: "schemaID", Schema: "/" + s}}
			s, err := loadSchema(reg, "schemaID", dontUseLocalSchemaFiles)
			is.NoErr(err)
			_, err = s.Validate(gojsonschema.NewGoLoader(map[string]interface{}{"name": "foo"}))
			is.NoErr(err)
		}
	})
}

func TestLoadSchemaRegistry(t *testing.T) {
	t.Run("LocalRegistry", func(t *testing.T) {
		is := is.New(t)
		r, err := loadSchemaRegistry(dontUseLocalSchemaFiles, localRegistryPath, "remoteRegistry")
		is.NoErr(err)
		if len(r) == 0 {
			t.Fatalf("len(schemaRegistry) want:>0 got:0")
		}
	})
	t.Run("LocalRegistryInvalidJSON", func(t *testing.T) {
		ts := serverForTests(`123`)
		defer ts.Close()
		_, err := loadSchemaRegistry(dontUseLocalSchemaFiles, "localRegistryPath", ts.URL)
		if err == nil {
			t.Fatalf("want:err got:nil")
		}
	})
	t.Run("RemoteRegistry", func(t *testing.T) {
		is := is.New(t)
		ts := serverForTests(`[{"id": "data-package", "schema": "schema1", "title":"title1", "schema_path":"path1", "specification":"spec1"}]`)
		defer ts.Close()
		m, err := loadSchemaRegistry(dontUseLocalSchemaFiles, "localRegistryPath", ts.URL)
		is.NoErr(err)
		if len(m) == 0 {
			t.Fatalf("len(schemaRegistry) want:>0 got:0")
		}
		is.Equal(m["data-package"], profileSpec{ID: "data-package", Schema: "schema1", Title: "title1", SchemaPath: "path1", Specification: "spec1"})
	})
	t.Run("InvalidRemoteRegistry", func(t *testing.T) {
		_, err := loadSchemaRegistry(dontUseLocalSchemaFiles, "localRegistryPath", "remoteRegistry")
		if err == nil {
			t.Fatalf("want:err got:nil")
		}
	})
}

func serverForTests(contents string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, contents)
	}))
}
