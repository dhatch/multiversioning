CFLAGS=-g -Werror -Wall -Wextra -w -std=c++0x
LIBS=-lnuma -lpthread -lrt -lcityhash -ltcmalloc_minimal -lprofiler
CXX=g++

INCLUDE=include
SRC=src
SOURCES=$(wildcard $(SRC)/*.cc $(SRC)/*.c)
OBJECTS=$(patsubst $(SRC)/%.cc,build/%.o,$(SOURCES))

TEST=test
TESTSOURCES=$(wildcard $(TEST)/*.cc)
TESTOBJECTS=$(patsubst test/%.cc,test/%.o,$(TESTSOURCES))

DEPSDIR:=.deps
DEPCFLAGS=-MD -MF $(DEPSDIR)/$*.d -MP

all:CFLAGS+=-DTESTING=0
all:LIBS+=-ltcmalloc_minimal
all:build/db

test:CFLAGS+=-DTESTING=1
test:build/tests

build/%.o: src/%.cc $(DEPSDIR)/stamp 
	@mkdir -p build
	@echo + cc $<
	@$(CXX) $(CFLAGS) $(DEPCFLAGS) -I$(INCLUDE) -c -o $@ $<

$(TESTOBJECTS):$(OBJECTS)

test/%.o: test/%.cc $(DEPSDIR)/stamp 
	@echo + cc $<
	@$(CXX) $(CFLAGS) -Wno-missing-field-initializers -Wno-conversion-null $(DEPCFLAGS) -I$(INCLUDE) -c -o $@ $<
	@rm $(DEPSDIR)/$*.d

start/%.o: start/%.cc $(DEPSDIR)/stamp 
	@echo + cc $<
	@$(CXX) $(CFLAGS) $(DEPCFLAGS) -I$(INCLUDE) -c -o $@ $<
	@rm $(DEPSDIR)/$*.d

build/db:start/main.o $(OBJECTS)
	@$(CXX) $(CFLAGS) -o $@ $^ $(LIBS)
	@rm -rf $(OBJECTS)

build/tests:$(OBJECTS) $(TESTOBJECTS)
	@$(CXX) $(CFLAGS) -o $@ $^ $(LIBS)

$(DEPSDIR)/stamp:
	@mkdir -p $(DEPSDIR)
	@touch $@


.PHONY: clean

clean:
	rm -rf build $(DEPSDIR) $(TESTOBJECTS) start/*.o





