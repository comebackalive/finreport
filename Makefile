VERSION = $(shell git describe --tags)

# minus means "include but do not fail"
-include ./.config.mk

run:
	clj -M:dev

ancient:
	clojure -M:dev:ancient

upgrade:
	clojure -M:dev:ancient --upgrade

clean:
	rm -rf target

release:
	git tag 1.$(shell git rev-list --count HEAD)

uber: target/finreport-$(VERSION).jar

target/finreport-%.jar: $(shell find src resources)
	clojure -Srepro -T:build uber
	@ls target/*.jar

deploy: target/finreport-$(shell git describe --tags).jar
	scp target/finreport-$(shell git describe --tags).jar report:
	ssh report -- 'sudo mv finreport*.jar /opt/finreport/finreport.jar && sudo systemctl restart finreport'
