#!/bin/bash

oasis setup

ocaml setup.ml -configure
ocaml setup.ml -build

#./test.native $1 $2
