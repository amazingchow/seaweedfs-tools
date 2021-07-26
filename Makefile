PROJECT      := github.com/amazingchow/seaweedfs-tools
SRC          := $(shell find . -type f -name '*.go' -not -path "./vendor/*")
VERSION      := v1.0.0
BRANCH       := $(shell git symbolic-ref --short -q HEAD)
BUILD        := $(shell git rev-parse --short HEAD)
TAG          := $(VERSION)-$(BRANCH)-$(BUILD)
TARGETS      := backup compactor transformer
TEST_TARGETS := check_how_many_needles_should_be_deleted generate-date-with-specified-last-modified-time
TAG_TARGETS  := backup-* compactor-* transformer-*
ALL_TARGETS  := $(TARGETS) $(TEST_TARGETS) $(TAG_TARGETS)

ifeq ($(race), 1)
	BUILD_FLAGS := -race
endif

ifeq ($(debug), 1)
	BUILD_FLAGS += -gcflags=all="-N -l"
endif

ifneq ($(BUILD_TAGS),)
	BUILD_FLAGS+= -tags '$(BUILD_TAGS)'
endif

ARCH := amd64
ifeq ($(sys_arch), arm64)
	ARCH := $(sys_arch)
endif

all: build

build: $(TARGETS) $(TEST_TARGETS)

$(TARGETS): $(SRC)
ifeq ("$(GOMODULEPATH)", "")
	@echo "no GOMODULEPATH env provided!!!"
	@exit 1
endif
	GOOS=linux GOARCH=$(ARCH) go build $(BUILD_FLAGS) $(GOMODULEPATH)/$(PROJECT)/cmd/$@

$(TEST_TARGETS): $(SRC)
ifeq ("$(GOMODULEPATH)", "")
	@echo "no GOMODULEPATH env provided!!!"
	@exit 1
endif
	GOOS=linux GOARCH=$(ARCH) go build $(BUILD_FLAGS) $(GOMODULEPATH)/$(PROJECT)/tools/$@

lint:
	@golangci-lint run --config=.golangci-lint.yml

tag:
	cp backup backup-$(TAG)
	cp compactor compactor-$(TAG)
	cp transformer transformer-$(TAG)
	@git tag $(TAG)

clean:
	rm -f $(ALL_TARGETS) 

.PHONY: all build tag clean
