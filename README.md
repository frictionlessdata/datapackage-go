[![Build Status](https://travis-ci.org/frictionlessdata/datapackage-go.svg?branch=master)](https://travis-ci.org/frictionlessdata/datapackage-go) [![Coverage Status](https://coveralls.io/repos/github/frictionlessdata/datapackage-go/badge.svg?branch=master)](https://coveralls.io/github/frictionlessdata/datapackage-go?branch=master) [![Go Report Card](https://goreportcard.com/badge/github.com/frictionlessdata/datapackage-go)](https://goreportcard.com/report/github.com/frictionlessdata/datapackage-go) [![Gitter chat](https://badges.gitter.im/gitterHQ/gitter.png)](https://gitter.im/frictionlessdata/chat) [![GoDoc](https://godoc.org/github.com/frictionlessdata/datapackage-go?status.svg)](https://godoc.org/github.com/frictionlessdata/datapackage-go/pkg)

# datapackage-go
A Go library for working with [Data Packages](http://specs.frictionlessdata.io/data-package/).

## Features

* [pkg.Package](https://godoc.org/github.com/frictionlessdata/datapackage-go/pkg#Package) class for working with data packages
* [Resource](https://godoc.org/github.com/frictionlessdata/datapackage-go/pkg#Resource) class for working with data resources
* [Valid](https://godoc.org/github.com/frictionlessdata/datapackage-go/pkg#Valid) function for validating data package descriptors

## Getting started

## Library Installation

This package uses [semantic versioning 2.0.0](http://semver.org/).

### Using dep

```sh
$ go get -u github.com/golang/dep/cmd/dep
$ dep init
$ dep ensure
```

### Using go get

```sh
$ go get -u github.com/frictionlessdata/datapackage-go/...
```

## Example

Code examples in this readme requires Go 1.8+. You could see even more example in [examples](https://github.com/frictionlessdata/datapackage-go/tree/master/examples) directory.

```go
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
pkg, err := FromString(descriptor)
if err != nil {
    panic(err)
}
res := pkg.GetResource("example")
contents, _ := res.ReadAll(csv.LoadHeaders())
fmt.Println(contents)
// [[180 18 Tony] [192 32 Jacob]]
```

## Main Features

### Packages

A [data package](http://frictionlessdata.io/specs/data-package/) is a collection of [resources](http://frictionlessdata.io/specs/data-resource/). The [datapackage.Package](https://godoc.org/github.com/frictionlessdata/datapackage-go/datapackage#Package) provides various capabilities like loading local or remote data package, saving a data package descriptor and many more.

Consider we have some local csv file and a JSON descriptor in a `data` directory:

> data/population.csv
```csv
city,year,population
london,2017,8780000
paris,2017,2240000
rome,2017,2860000
```

> data/datapackage.json
```json
{
    "name": "world",
    "resources": [
      {
        "name": "population",
        "path": "population.csv",
        "profile":"tabular-data-resource",
        "schema": {
          "fields": [
            {"name": "city", "type": "string"},
            {"name": "year", "type": "integer"},
            {"name": "population", "type": "integer"}
          ]
        }
      }
    ]
  }
```

Let's create a data package based on this data using the [datapackage.Package](https://godoc.org/github.com/frictionlessdata/datapackage-go/datapackage#Package) class:

```go
pkg, err := datapackage.Load("data/datapackage.json")
// Error validation.
```

Once the data package is loaded, we could use the [datapackage.Resource](https://godoc.org/github.com/frictionlessdata/datapackage-go/datapackage#Resource) class to read and iterate over the data resource's contents:

```go
books := pkg.GetResource("books")
contents, _ := books.ReadAll()
fmt.Println(contents)
// [[london 2017 8780000] [paris 2017 2240000] [rome 20172860000]]
```