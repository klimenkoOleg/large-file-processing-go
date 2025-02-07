PROJECTNAME=$(shell basename "$(PWD)")

CLI_MAIN_FOLDER=./cmd
BIN_FOLDER=bin
BIN_NAME=${PROJECTNAME}

# Make is verbose in Linux. Make it silent.
MAKEFLAGS += --silent
# LDFLAGS=-X main.buildDate=`date -u +%Y-%m-%dT%H:%M:%SZ` -X main.version=`scripts/version.sh`
LDFLAGS=

## setup: install all build dependencies for ci
setup: mod-download

## compile: compiles project in current system
compile: clean generate fmt vet test build

clean:
	@echo "  >  Cleaning "
	@-rm -rf ${BIN_FOLDER}/${BIN_NAME} merged_*.tsv temp_*.tsv output.tsv \
		&& go clean ./...

build:
	@echo "  >  Building binary"
	@go build \
		-ldflags="${LDFLAGS}" \
		-o ${BIN_FOLDER}/${BIN_NAME} \
		"${CLI_MAIN_FOLDER}"		

generate:
	@echo "  >  Go generate"
	@if !type "stringer" > /dev/null 2>&1; then \
		go install golang.org/x/tools/cmd/stringer@latest; \
	fi
	@go generate ./...		

mod-download:
	@echo "  >  Download dependencies..."
	@go mod download && go mod tidy

test:
	@echo "  >  Executing unit tests"
	@go test -v -timeout 60s -race ./...	


vet:
	@echo "  >  Checking code with vet"
	@go vet ./...

run:
	@echo "  >  Running "
	@go run \
		"${CLI_MAIN_FOLDER}"/main.go
	@-rm merged_*.tsv temp_*.tsv \
