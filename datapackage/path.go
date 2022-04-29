package datapackage

import (
	"fmt"
	"net/url"
	"path"
	"path/filepath"
	"strings"
)

func parseRemotePath(path string) (*url.URL, bool) {
	u, err := url.Parse(path)
	return u, err == nil && u.Scheme != "" && u.Host != ""
}

func joinPaths(basePath, finalPath string) string {
	if u, isRemote := parseRemotePath(basePath); isRemote {
		u.Path = path.Join(u.Path, finalPath)
		return u.String()
	}
	return filepath.Join(basePath, finalPath)
}

func getBasepath(p string) string {
	// If it is a remote-like URL, should not treat slashs in a system OS-dependent way.
	if u, isRemote := parseRemotePath(p); isRemote {
		uStr := strings.TrimSuffix(u.String(), "/")
		uPath := strings.TrimSuffix(u.Path, "/")
		if uPath == "" {
			return fmt.Sprintf("%s/", uStr)
		}
		return strings.TrimSuffix(uStr, path.Base(u.String()))
	}
	// It local path.
	return filepath.Dir(p)
}
