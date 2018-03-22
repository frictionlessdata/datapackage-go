package validator

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/matryer/is"
)

const simpleSchema = `{
	"$schema": "http://json-schema.org/draft-04/schema#",
	"type": "object",
	"oneOf": [{"required": ["name"]}]
}`

var localLoader = LocalRegistryLoader(localRegistryPath, true /* in memory only*/)

func TestDescriptorValidator_IsValid(t *testing.T) {
	t.Run("ValidProfile", func(t *testing.T) {
		is := is.New(t)
		v, err := New("data-package", localLoader)
		is.NoErr(err)
		is.NoErr(v.Validate(map[string]interface{}{"resources": []interface{}{map[string]interface{}{"name": "res1", "path": "foo.csv"}}}))
	})
	t.Run("InvalidProfile", func(t *testing.T) {
		is := is.New(t)
		v, err := New("data-package", localLoader)
		is.NoErr(err)
		is.True(v.Validate(map[string]interface{}{"resources": []interface{}{map[string]interface{}{"name": "res1"}}}) != nil)
	})
}

func TestNew(t *testing.T) {
	t.Run("ThirdPartyRemoteSchema", func(t *testing.T) {
		is := is.New(t)
		ts := serverForTests(simpleSchema)
		defer ts.Close()

		v, err := New(ts.URL)
		is.NoErr(err)
		is.NoErr(v.Validate(map[string]interface{}{"name": "foo"}))
	})
	t.Run("RemoteSchemaRegistry", func(t *testing.T) {
		is := is.New(t)
		schServer := serverForTests(simpleSchema)
		defer schServer.Close()
		regServer := serverForTests(fmt.Sprintf(`[{"id":"schemaID", "schema":"%s"}]`, schServer.URL))
		defer regServer.Close()

		v, err := New("schemaID", RemoteRegistryLoader(regServer.URL))
		is.NoErr(err)
		is.NoErr(v.Validate(map[string]interface{}{"name": "foo"}))

		_, err = New("foo", RemoteRegistryLoader(regServer.URL))
		if err == nil {
			t.Fatalf("want:err got:nil")
		}
	})
	t.Run("LocalRegistry", func(t *testing.T) {
		is := is.New(t)
		profiles := []string{
			"data-package",
			"data-resource",
			"fiscal-data-package",
			"table-schema",
			"tabular-data-package",
			"tabular-data-resource",
		}
		loader, err := localLoader()
		is.NoErr(err)
		for _, p := range profiles {
			_, err := loader.GetValidator(p)
			is.NoErr(err)
		}
	})
	t.Run("LocalInvalidProfile", func(t *testing.T) {
		_, err := New("boo", localLoader)
		if err == nil {
			t.Fatalf("want:err got:nil")
		}
	})
	t.Run("InvalidRegistryJSON", func(t *testing.T) {
		ts := serverForTests(`123`)
		defer ts.Close()
		_, err := RemoteRegistryLoader(ts.URL)()
		if err == nil {
			t.Fatalf("want:err got:nil")
		}
	})
	t.Run("InvalidRemoteRegistryURL", func(t *testing.T) {
		_, err := RemoteRegistryLoader("http://127.0.0.1/bar")()
		if err == nil {
			t.Fatalf("want:err got:nil")
		}
	})
}

type neverValidValidator struct{}

func (v neverValidValidator) Validate(map[string]interface{}) error { return fmt.Errorf("never valid") }

type neverValidRegistry struct{}

func (v neverValidRegistry) GetValidator(profile string) (DescriptorValidator, error) {
	return &neverValidValidator{}, nil
}

func TestFallbackRegistryLoader(t *testing.T) {
	t.Run("FallingBackOnLocal", func(t *testing.T) {
		is := is.New(t)
		loader, err := FallbackRegistryLoader(RemoteRegistryLoader("http://127.0.0.1/bar"), localLoader)()
		is.NoErr(err)
		v, err := loader.GetValidator("data-package")
		is.NoErr(err)
		is.NoErr(v.Validate(map[string]interface{}{"resources": []interface{}{map[string]interface{}{"name": "res1", "path": "foo.csv"}}}))
	})
	t.Run("TwoValidsShouldPickFirst", func(t *testing.T) {
		is := is.New(t)
		loader, err := FallbackRegistryLoader(InMemoryLoader(), func() (Registry, error) { return &neverValidRegistry{}, nil })()
		is.NoErr(err)
		v, err := loader.GetValidator("data-package")
		is.NoErr(err)
		is.NoErr(v.Validate(map[string]interface{}{"resources": []interface{}{map[string]interface{}{"name": "res1", "path": "foo.csv"}}}))
	})
	t.Run("NoLoader", func(t *testing.T) {
		_, err := FallbackRegistryLoader()()
		if err == nil {
			t.Fatalf("want:err got:nil")
		}
	})
	t.Run("AllErrors", func(t *testing.T) {
		_, err := FallbackRegistryLoader(RemoteRegistryLoader("http://127.0.0.1/bar"))()
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
