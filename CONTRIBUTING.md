# Contributing to datapackage-go

Found a problem and would like to fix it? Have that great idea and would love to see it done? Let's do it!

> Please open an issue before start working

That could save a lot of time from everyone and we are super happy to answer questions and help you alonge the way.

This project shares Go's code of conduct [values](https://golang.org/conduct#values) and [unwelcomed behavior](https://golang.org/conduct#unwelcome_behavior). Not sure what those mean or why we need those? Please give yourself a few minutes to get acquainted to those topics.

* Before start coding:
     * Fork and pull the latest version of the master branch
     * Make sure you have go 1.8+ installed and you're using it
     * Install [dep](https://github.com/golang/dep) and ensure the dependencies are updated

```sh
$ go get -u github.com/golang/dep/cmd/dep
$ dep ensure
```

* Requirements
    * Compliance with [these guidelines](https://code.google.com/p/go-wiki/wiki/CodeReviewComments)
    * Good unit test coverage
    * [Good commit messages](http://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html)

* Before sending the PR

```sh
$ cd $GOPATH/src/github.com/frictionlessdata/datapackage-go
$ ./fmt.sh
$ go test ./..
```

If all tests pass, you're ready to send the PR! :D

## Updating local data pakcage schema registry

To speed up development and usage, datapackage-go comes with a local copy of the [Data Package Schema Registry](http://frictionlessdata.io/schemas/registry.json). As Go does not support resources out of the box, we are using [esc]
(https://github.com/mjibson/esc). Esc generates nice, gzipped strings, one per file generates a set of go functions that
allow us to access the schema files from go code.

To add or update JSONSchemas from the local registry one first needs to install esc.

```sh
$ go get github.com/mjibson/esc
```

After all editing/adding, simply invoke `esc`

```sh
$ cd $GOPATH/src/github.com/frictionlessdata/datapackage-go
$ cd pkg/profile_cache
$ esc -o profile_cache.go -pkg profile_cache -ignore profile_cache.go .
$ cd ../..
$ go test ./..
```


And create a PR with the changes.
