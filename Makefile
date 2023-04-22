CFLAGS := -std=c++23 -fvisibility=hidden -lpthread -Wall -Wextra

ifeq (,$(CONFIGURATION))
	CONFIGURATION := basic
else
	CONFIGURATION := tree
endif

CFLAGS += -g
CFLAGS += -O3 -g
CXX = mpic++

HEADERS := src/*.h

.SUFFIXES:
.PHONY: all clean

all: mini-basic-map-reduce mini-tree-map-reduce

mini-$(CONFIGURATION)-map-reduce: src/*.cpp src/*.h
	$(CXX) -o $@ $(CFLAGS) $(FILES)
	@echo Compilation successful!

clean:
	rm -rf ./mini-*

FILES = src/*.cpp