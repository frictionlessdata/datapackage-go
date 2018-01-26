[![Build Status](https://travis-ci.org/frictionlessdata/datapackage-go.svg?branch=master)](https://travis-ci.org/frictionlessdata/datapackage-go) [![Coverage Status](https://coveralls.io/repos/github/frictionlessdata/datapackage-go/badge.svg?branch=master)](https://coveralls.io/github/frictionlessdata/datapackage-go?branch=master) [![Go Report Card](https://goreportcard.com/badge/github.com/frictionlessdata/datapackage-go)](https://goreportcard.com/report/github.com/frictionlessdata/datapackage-go) [![Gitter chat](https://badges.gitter.im/gitterHQ/gitter.png)](https://gitter.im/frictionlessdata/chat) [![GoDoc](https://godoc.org/github.com/frictionlessdata/datapackage-go?status.svg)](https://godoc.org/github.com/frictionlessdata/datapackage-go) [![Sourcegraph](https://sourcegraph.com/github.com/frictionlessdata/datapackage-go/-/badge.svg)](https://sourcegraph.com/github.com/frictionlessdata/datapackage-go?badge)

# datapackage-go
A Go library for working with [Data Packages](http://specs.frictionlessdata.io/data-package/).

<!-- TOC -->

- [datapackage-go](#datapackage-go)
    - [Install](#install)
    - [Main Features](#main-features)
        - [Loading and validating data package descriptors](#loading-and-validating-data-package-descriptors)
        - [Accessing data package resources](#accessing-data-package-resources)
        - [Loading zip bundles](#loading-zip-bundles)
        - [Creating a zip bundle with the data package.](#creating-a-zip-bundle-with-the-data-package)
        - [CSV dialect support](#csv-dialect-support)
        - [Loading multipart resources](#loading-multipart-resources)
        - [Manipulating data packages programatically](#manipulating-data-packages-programatically)

<!-- /TOC -->

## Install

```sh
$ go get -u github.com/frictionlessdata/datapackage-go/...
```

## Main Features

### Loading and validating data package descriptors

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

### Accessing data package resources

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
### Loading zip bundles

It is very common to store the data in zip bundles containing the descriptor and data files. Those are natively supported by our the [datapackage.Load](https://godoc.org/github.com/frictionlessdata/datapackage-go/datapackage#Load) method. For example, lets say we have the following `package.zip` bundle:

    |- package.zip
        |- datapackage.json
        |- data.csv

We could load this package by simply:

```go
pkg, err := datapackage.Load("package.zip")
// Check error.
```

And the library will unzip the package contents to a temporary directory and wire everything up for us.

A complete example can be found [here](https://github.com/frictionlessdata/datapackage-go/tree/master/examples/load_zip).

### Creating a zip bundle with the data package.

You could also easily create a zip file containing the descriptor and all the data resources. Let's say you have a [datapackage.Package](https://godoc.org/github.com/frictionlessdata/datapackage-go/datapackage#Package) instance, to create a zip file containing all resources simply:

```go
err := pkg.Zip("package.zip")
// Check error.
```

This call also download remote resources. A complete example can be found [here](https://github.com/frictionlessdata/datapackage-go/tree/master/examples/zip)

### CSV dialect support

Basic support for configuring [CSV dialect](http://frictionlessdata.io/specs/csv-dialect/) has been added. In particular `delimiter`, `skipInitialSpace` and `header` fields are supported. For instance, lets assume the population file has a different field delimiter:

> data/population.csv
```csv
city,year,population
london;2017;8780000
paris;2017;2240000
rome;2017;2860000
```

One could easily parse by adding following `dialect` property to the `world` resource:

```json
    "dialect":{
        "delimiter":";"
    }
```

A complete example can be found [here](https://github.com/frictionlessdata/datapackage-go/tree/master/examples/load).

### Loading multipart resources

Sometimes you have data scattered across many local or remote files. Datapackage-go offers an easy way you to deal all those file as one big
file. We call it multipart resources. To use this feature, simply list your files in the `path` property of the resource. For example, lets
say our population data is now split between north and south hemispheres. To deal with this, we only need change to change the package descriptor:

> data/datapackage.json
```json
{
    "name": "world",
    "resources": [
      {
        "name": "population",
        "path": ["north.csv","south.csv"],
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

And all the rest of the code would still be working. 

A complete example can be found [here](https://github.com/frictionlessdata/datapackage-go/tree/master/examples/multipart).


### Manipulating data packages programatically

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
