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

deploy:
	test -f target/finreport-$(shell git describe --tags).jar
	scp target/finreport-$(shell git describe --tags).jar report:
	ssh report -- 'sudo mv finreport*.jar /opt/finreport/finreport.jar && sudo systemctl restart finreport'
