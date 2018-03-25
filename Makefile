SHELL=/bin/bash -eou pipefail
GOTOOLS= github.com/alecthomas/gometalinter github.com/wadey/gocovmerge
PKGS=$(shell go list ./... | grep -v /vendor/)
PKG_SUBDIRS=$(shell go list ./... | grep -v /vendor/ | sed -r 's|github.com/elixirhealth/courier/||g' | sort)
GIT_STATUS_SUBDIRS=$(shell git status --porcelain | grep -e '\.go$$' | sed -r 's|^...(.+)/[^/]+\.go$$|\1|' | sort | uniq)
GIT_DIFF_SUBDIRS=$(shell git diff develop..HEAD --name-only | grep -e '\.go$$' | sed -r 's|^(.+)/[^/]+\.go$$|\1|' | sort | uniq)
GIT_STATUS_PKG_SUBDIRS=$(shell echo $(PKG_SUBDIRS) $(GIT_STATUS_SUBDIRS) | tr " " "\n" | sort | uniq -d)
GIT_DIFF_PKG_SUBDIRS=$(shell echo $(PKG_SUBDIRS) $(GIT_DIFF_SUBDIRS) | tr " " "\n" | sort | uniq -d)
SERVICE_BASE_PKG=github.com/elixirhealth/service-base

.PHONY: bench build

acceptance:
	@echo "--> Running acceptance tests"
	@mkdir -p artifacts
	@go test -tags acceptance -v github.com/elixirhealth/courier/pkg/acceptance 2>&1 | tee artifacts/acceptance.log

build:
	@echo "--> Running go build"
	@go build $(PKGS)

build-static:
	@echo "--> Running go build for static binary"
	@./vendor/$(SERVICE_BASE_PKG)/scripts/build-static deploy/bin/courier

demo:
	@echo "--> Running demo"
	@./pkg/acceptance/local-demo.sh

docker-image:
	@echo "--> Building docker image"
	@docker build --rm=false -t gcr.io/elxir-core-infra/courier:snapshot deploy

fix:
	@echo "--> Running goimports"
	@find . -name *.go | grep -v /vendor/ | xargs goimports -l -w

get-deps:
	@echo "--> Getting dependencies"
	@go get -u github.com/golang/dep/cmd/dep
	@dep ensure
	@go get -u -v $(GOTOOLS)
	@gometalinter --install

install-git-hooks:
	@echo "--> Installing git-hooks"
	@vendor/$(SERVICE_BASE_PKG)/scripts/install-git-hooks.sh

lint:
	@echo "--> Running gometalinter"
	@gometalinter $(PKG_SUBDIRS) --config="vendor/$(SERVICE_BASE_PKG)/.gometalinter.json" --deadline=5m

lint-diff:
	@echo "--> Running gometalinter on packages with uncommitted changes"
	@echo $(GIT_STATUS_PKG_SUBDIRS) | tr " " "\n"
	@echo $(GIT_STATUS_PKG_SUBDIRS) | xargs gometalinter --config="vendor/$(SERVICE_BASE_PKG)/.gometalinter.json" --deadline=5m

proto:
	@echo "--> Running protoc"
	@protoc pkg/courierapi/courier.proto -I. -I vendor/ --go_out=plugins=grpc:.

test:
	@echo "--> Running go test"
	@go test -race $(PKGS)
