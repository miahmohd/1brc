# Rust project name
#
PROJECT_NAME := rust1brc

# Output directory
# OUT_DIR := target

SRC_DIR := src
SRC_FILES := $(wildcard $(SRC_DIR)/*.rs)


# Target executable
TARGET := target/release-with-debug/$(PROJECT_NAME)

# Profiling target
PROFILE_TARGET := $(OUT_DIR)/perf.data

.PHONY: all clean run profile

all: $(TARGET)

$(TARGET): $(SRC_FILES)
	cargo build --profile=release-with-debug

run: $(TARGET)
	./$(TARGET) measurements.txt

bench: $(TARGET)
	hyperfine -w 1 -r 5 './$(TARGET) measurements.txt'

profile: all
	perf record --call-graph dwarf -F99 ./$(TARGET) measurements.txt
	perf script -F +pid > ./test.perf

