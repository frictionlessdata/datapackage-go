[![Build Status](https://travis-ci.org/frictionlessdata/datapackage-go.svg?branch=master)](https://travis-ci.org/frictionlessdata/datapackage-go) [![Coverage Status](https://coveralls.io/repos/github/frictionlessdata/datapackage-go/badge.svg?branch=master)](https://coveralls.io/github/frictionlessdata/datapackage-go?branch=master) [![Go Report Card](https://goreportcard.com/badge/github.com/frictionlessdata/datapackage-go)](https://goreportcard.com/report/github.com/frictionlessdata/datapackage-go) [![Gitter chat](https://badges.gitter.im/gitterHQ/gitter.png)](https://gitter.im/frictionlessdata/chat) [![GoDoc](https://godoc.org/github.com/frictionlessdata/datapackage-go?status.svg)](https://godoc.org/github.com/frictionlessdata/datapackage-go)

# datapackage-go
A Go library for working with [Data Packages](http://specs.frictionlessdata.io/data-package/).

## Features

* [Package](https://godoc.org/github.com/frictionlessdata/datapackage-go/datapackage#Package) class for working with data packages
* [Resource](https://godoc.org/github.com/frictionlessdata/datapackage-go/datapackage#Resource) class for working with data resources
* [Valid](https://godoc.org/github.com/frictionlessdata/datapackage-go/validator#IsValid) function for validating data package descriptors

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
    "resources": [{
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
    }]
}`
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

### Data description and processing

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
// Check error.
```

> The [datapackage.Load](https://godoc.org/github.com/frictionlessdata/datapackage-go/datapackage#Load) function also works for remote and zip file packages. Please check the [load_zip example](https://github.com/frictionlessdata/datapackage-go/tree/master/examples/load_zip).

Once the data package is loaded, we could use the [datapackage.Resource](https://godoc.org/github.com/frictionlessdata/datapackage-go/datapackage#Resource) class to read data resource's contents:

```go
resource := pkg.GetResource("population")
contents, _ := resource.ReadAll()
fmt.Println(contents)
// [[london 2017 8780000] [paris 2017 2240000] [rome 20172860000]]
```

Or you could cast to Go types, making it easier to perform further processing:

```go
type Population struct {
    City string `tableheader:"city"`
    Year  string `tableheader:"year"`
    Population   int    `tableheader:"population"`
}

var cities []Population
resource.Cast(&cities, csv.LoadHeaders())
fmt.Printf("+v", cities)
// [{City:london Year:2017 Population:8780000} {City:paris Year:2017 Population:2240000} {City:rome Year:2017 Population:2860000}]
```

Finally, if the data is to big to be loaded at once or if you would like to perform line-by-line processing, you could iterate through the resource contents:

```go
iter, _ := resource.Iter(csv.LoadHeaders())
sch, _ := resource.GetSchema()
for iter.Next() {
    var p Population
    sch.CastRow(iter.Row(), &cp)
    fmt.Printf("%+v\n", p)
}
// {City:london Year:2017 Population:8780000
// {City:paris Year:2017 Population:2240000}
// {City:rome Year:2017 Population:2860000}]
```

### Manipulating data packages

The datapackage-go library also makes it easy to save packages. Let's say you're creating a program that produces data packages and would like to add or remove resource:

```go
descriptor := map[string]interface{}{
    "resources": []interface{}{
        map[string]interface{}{
            "name":    "books",
            "path":    "books.csv",
            "format":  "csv",
            "profile": "tabular-data-resource",
            "schema": map[string]interface{}{
                "fields": []interface{}{
                    map[string]interface{}{"name": "author", "type": "string"},
                    map[string]interface{}{"name": "title", "type": "string"},
                    map[string]interface{}{"name": "year", "type": "integer"},
                },
            },
        },
    },
}
pkg, err := datapackage.New(descriptor, ".", validator.InMemoryLoader())
if err != nil {
    panic(err)
}
// Removing resource.
pkg.RemoveResource("books")

// Adding new resource.
pkg.AddResource(map[string]interface{}{
    "name":    "cities",
    "path":    "cities.csv",
    "format":  "csv",
    "profile": "tabular-data-resource",
    "schema": map[string]interface{}{
        "fields": []interface{}{
            map[string]interface{}{"name": "city", "type": "string"},
            map[string]interface{}{"name": "year", "type": "integer"},
            map[string]interface{}{"name": "population", "type": "integer"}
        },
    },
})

// Printing resource contents.
cities, _ := pkg.GetResource("cities").ReadAll()
fmt.Println(cities)
// [[london 2017 8780000] [paris 2017 2240000] [rome 20172860000]]
```

### Creating a zip bundle with the data package.

You could also easily create a zip file containing the descriptor and all the data resources, which could loaded afterwards:

```go
pkg.Zip("cities.zip")
```