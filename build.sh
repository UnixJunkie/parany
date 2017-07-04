#!/bin/bash

oasis setup

ocaml setup.ml -configure
ocaml setup.ml -build
# ocamlfind remove parany
# ocaml setup.ml -install

#./test.native $1 $2
