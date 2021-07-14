PROJECT     := github.com/amazingchow/seaweedfs-tools
SRC         := $(shell find . -type f -name '*.go' -not -path "./vendor/*")
TARGETS     := transformer
ALL_TARGETS := $(TARGETS)

ifeq ($(race), 1)
	BUILD_FLAGS := -race
endif

ifeq ($(debug), 1)
	BUILD_FLAGS += -gcflags=all="-N -l"
endif

all: build

build: $(ALL_TARGETS)

$(TARGETS): $(SRC)
ifeq ("$(GOMODULEPATH)", "")
	@echo "no GOMODULEPATH env provided!!!"
	@exit 1
endif
	go build $(BUILD_FLAGS) $(GOMODULEPATH)/$(PROJECT)/cmd/$@

clean:
	rm -f $(ALL_TARGETS)

run:
	env ENCRYPTION_KEY=TEpSZVlpTURwRENuS0JkNXBGZzQzUT09 ./transformer -verbose=true -collection=faces -vid=1 \
		-src=/home/zhoujian2/seaweedfs/vdata \
		-dst=/home/zhoujian2/seaweedfs/vdata-backup

.PHONY: all build clean
