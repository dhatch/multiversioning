CFLAGS=-g -Werror -Wextra -std=c++0x
LIBS=-lnuma -lpthread -lrt -lcityhash -ltcmalloc_minimal
CXX=g++

INCLUDE=include
SRC=src
SOURCES=$(wildcard $(SRC)/*.cc $(SRC)/*.c)
OBJECTS=$(patsubst $(SRC)/%.cc,build/%.o,$(SOURCES))

DEPSDIR:=.deps
DEPCFLAGS=-MD -MF $(DEPSDIR)/$*.d -MP

all:build/tests

build/%.o: src/%.cc $(DEPSDIR)/stamp 
	@mkdir -p build
	@echo + cc $<
	@$(CXX) $(CFLAGS) $(DEPCFLAGS) -I$(INCLUDE) -c -o $@ $<


build/tests:$(OBJECTS)
	@$(CXX) $(CFLAGS) -o $@ $^ $(LIBS) -lgtest

$(DEPSDIR)/stamp:
	@mkdir -p $(DEPSDIR)
	@touch $@

.PHONY: clean

clean:
	rm -rf build $(DEPSDIR)





