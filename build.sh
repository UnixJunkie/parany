#!/bin/bash

set -e

# clear old oasis-generated files
\rm setup.*

oasis setup

ocaml setup.ml -configure --prefix `opam config var prefix`
ocaml setup.ml -build
# ocaml setup.ml -uninstall
# ocaml setup.ml -install

#./test.native $1 $2
