VERSION = $(shell cat VERSION)

# minus means "include but do not fail"
-include ./.config.mk

run:
	clj -M:dev

ancient:
	clojure -M:dev:ancient

upgrade:
	clojure -M:dev:ancient --upgrade

uber:
	clojure -Srepro -T:build uber
	@ls target/*.jar

clean:
	rm -rf target

release:
	git tag 1.$(shell git rev-list --count HEAD)
