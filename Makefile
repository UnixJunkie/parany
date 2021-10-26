.PHONY: build build-test clean edit install uninstall reinstall test

build:
	dune build @install -j 16

build-test: build
	dune build src/test.exe
	dune build src/test_parmap.exe

clean:
	rm -rf _build

edit:
	emacs src/*.ml &

install: build
	dune install

uninstall:
	dune uninstall

reinstall: uninstall install

test: build-test
	./test.sh
