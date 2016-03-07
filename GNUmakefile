CFLAGS=-O2 -g -Wall -Wextra -Werror -std=c++0x -Wno-sign-compare 
CFLAGS+=-DSNAPSHOT_ISOLATION=0 -DSMALL_RECORDS=0 -DREAD_COMMITTED=1
LIBS=-lnuma -lpthread -lrt -lcityhash 
CXX=g++

INCLUDE=include
SRC=src
SOURCES:=$(wildcard $(SRC)/*.cc $(SRC)/*.c)
HEKATON:=$(wildcard $(SRC)/hek*.cc $(SRC)/hek*.c)
HEK_OBJ:=$(patsubst $(SRC)/%.cc,build/%.o,$(HEKATON))
OBJECTS:=$(patsubst $(SRC)/%.cc,build/%.o,$(SOURCES))
START:=$(wildcard start/*.cc start/*.c)
START_OBJECTS:=$(patsubst start/%.cc,start/%.o,$(START))
TEST:=test
TESTSOURCES:=$(wildcard $(TEST)/*.cc)
TESTOBJECTS:=$(patsubst test/%.cc,test/%.o,$(TESTSOURCES))
NON_HEK_OBJECTS:=$(filter-out $(HEK_OBJ),$(OBJECTS))
NON_MAIN_STARTS:=$(filter-out start/main.o,$(START_OBJECTS))


DEPSDIR:=.deps
DEPCFLAGS=-MD -MF $(DEPSDIR)/$*.d -MP

all:CFLAGS+=-DTESTING=0 -DUSE_BACKOFF=1 -fno-omit-frame-pointer
all:env build/db

test:CFLAGS+=-DTESTING=1 -DUSE_BACKOFF=1 
test:env build/tests

-include $(wildcard $(DEPSDIR)/*.d)

build/%.o: src/%.cc $(DEPSDIR)/stamp GNUmakefile
	@mkdir -p build
	@echo + cc $<
	@$(CXX) $(CFLAGS) $(DEPCFLAGS) -I$(INCLUDE) -c -o $@ $<

$(TESTOBJECTS):$(OBJECTS)

test/%.o: test/%.cc $(DEPSDIR)/stamp GNUmakefile
	@echo + cc $<
	@$(CXX) $(CFLAGS) -Wno-missing-field-initializers -Wno-conversion-null $(DEPCFLAGS) -Istart -I$(SRC) -I$(INCLUDE) -c -o $@ $<

start/%.o: start/%.cc $(DEPSDIR)/stamp GNUmakefile
	@echo + cc $<
	@$(CXX) $(CFLAGS) $(DEPCFLAGS) -I$(INCLUDE) -Istart -c -o $@ $<

build/db:$(START_OBJECTS) $(OBJECTS)
	@$(CXX) $(CFLAGS) -o $@ $^ $(LIBS)

build/tests:$(OBJECTS) $(TESTOBJECTS) $(NON_MAIN_STARTS)
	@$(CXX) $(CFLAGS) -o $@ $^ $(LIBS)

$(DEPSDIR)/stamp:
	@mkdir -p $(DEPSDIR)
	@touch $@

.PHONY: clean env

clean:
	rm -rf build $(DEPSDIR) $(TESTOBJECTS) start/*.o
