
# goberkeleydb

BerkeleyDB is a good old key value DB library.
`goberkeleydb` is a BerkeleyDB binding for go. 

## install

BerkeleyDB library and the header files are required.
Please install them before run `go get`.
For example:

```sh
# Debian
$ apt-get install libdb-dev

# RHEL
$ dnf install libdb-dev

# Mac OS X via homebrew
$ brew install berkeley-db
```

then, `go get`

```sh
$ go get -u github.com/ajiyoshi-vg/goberkeleydb/bdb
```

## misc

Some apis are not implemented yet.
You can implement [any ohter apis](https://docs.oracle.com/cd/E17276_01/html/api_reference/C/index.html) you need.
