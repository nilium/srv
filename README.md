srv
===
[![GoDoc](https://godoc.org/go.spiff.io/srv?status.svg)](https://godoc.org/go.spiff.io/srv)

go.spiff.io/srv is a simple package for looking up and dialing hosts described
in SRV records. It doesn't do much else, but does include a srvproxy command
that provides a very basic TCP proxy for services described in SRV records.

Support for UDP in srvproxy is not yet available.


Install
-------

    $ go get go.spiff.io/srv

To install srvproxy:

    $ go get go.spiff.io/srv/cmd/srvproxy


License
-------

This package and subpackages are licensed under a BSD 2-Clause license.
