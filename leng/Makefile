.PHONY:
build:
	clojure -T:build ci

.PHONY:
install: build
	clojure -T:build install

unit-test:
	clojure -X:test

fmt-fix:
	cljfmt fix