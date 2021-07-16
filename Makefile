PROJECT     := github.com/amazingchow/seaweedfs-tools
SRC         := $(shell find . -type f -name '*.go' -not -path "./vendor/*")
VERSION     := v1.0.0
BRANCH      := $(shell git symbolic-ref --short -q HEAD)
BUILD       := $(shell git rev-parse --short HEAD)
TAG         := $(VERSION)-$(BRANCH)-$(BUILD)
TARGETS     := backup compactor transformer
ALL_TARGETS := $(TARGETS)

ifeq ($(race), 1)
	BUILD_FLAGS := -race
endif

ifeq ($(debug), 1)
	BUILD_FLAGS += -gcflags=all="-N -l"
endif

ARCH := amd64
ifeq ($(sys_arch), arm64)
	ARCH := $(sys_arch)
endif

all: build

build: $(ALL_TARGETS)

$(TARGETS): $(SRC)
ifeq ("$(GOMODULEPATH)", "")
	@echo "no GOMODULEPATH env provided!!!"
	@exit 1
endif
	GOOS=linux GOARCH=$(ARCH) go build $(BUILD_FLAGS) $(GOMODULEPATH)/$(PROJECT)/cmd/$@

lint:
	@golangci-lint run --config=.golangci-lint.yml

tag:
	@git tag $(TAG)

clean:
	rm -f $(ALL_TARGETS)

.PHONY: all build tag clean
