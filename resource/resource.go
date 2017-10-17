// Package resource implements the specification: https://specs.frictionlessdata.io/data-resource/
package resource

import (
	"fmt"
	"net/url"
	"path"
	"strings"
)

const (
	pathProp = "path"
	dataProp = "data"
)

// Resource describes a data resource such as an individual file or table.
type Resource struct {
	Descriptor map[string]interface{} `json:"-"`
	Path       []string
}

// New creates a new Resource from the passed-in descriptor.
func New(d map[string]interface{}) (*Resource, error) {
	if d[pathProp] == nil && d[dataProp] == nil {
		return nil, fmt.Errorf("either path or data properties MUST be set to strings. Descriptor:%v", d)
	}
	r := Resource{}
	pathI := d[pathProp]
	switch pathI.(type) {
	default:
		return nil, fmt.Errorf("path MUST be a string or an array of strings. Descriptor:%v", d)
	case string:
		if p, ok := pathI.(string); ok {
			r.Path = append(r.Path, p)
		}
	case []string:
		r.Path = append(r.Path, pathI.([]string)...)
	}
	for _, p := range r.Path {
		u, err := url.Parse(p)
		if err == nil {
			if u.Scheme != "" {
				if u.Scheme != "" && u.Scheme != "http" && u.Scheme != "https" {
					return nil, fmt.Errorf("URLs MUST be fully qualified. MUST be using either http or https scheme. Descriptor:%v", d)
				}
				continue // valid URL path.
			}
		}
		if path.IsAbs(p) || strings.HasPrefix(path.Clean(p), "..") {
			return nil, fmt.Errorf("absolute paths (/) and relative parent paths (../) MUST NOT be used. Descriptor:%v", d)
		}
	}
	// TODO: implement the following restriction:
	// It is NOT permitted to mix fully qualified URLs and relative paths in a path array: strings MUST either all be relative paths or all URLs.
	return &r, nil
}
