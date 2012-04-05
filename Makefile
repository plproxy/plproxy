EXTENSION  = plproxy

# sync with NEWS, META.json, plproxy.control, debian/changelog
DISTVERSION = 2.4rc1
EXTVERSION = 2.4.0

# set to 1 to disallow functions containing SELECT
NO_SELECT = 0

# libpq config
PG_CONFIG = pg_config
PQINCSERVER = $(shell $(PG_CONFIG) --includedir-server)
PQINC = $(shell $(PG_CONFIG) --includedir)
PQLIB = $(shell $(PG_CONFIG) --libdir)

# module setup
MODULE_big = $(EXTENSION)
SRCS = src/cluster.c src/execute.c src/function.c src/main.c \
       src/query.c src/result.c src/type.c src/poll_compat.c src/aatree.c
OBJS = src/scanner.o src/parser.tab.o $(SRCS:.c=.o)
EXTRA_CLEAN = src/scanner.[ch] src/parser.tab.[ch] libplproxy.* plproxy.so
SHLIB_LINK = -L$(PQLIB) -lpq

HDRS = src/plproxy.h src/rowstamp.h src/aatree.h src/poll_compat.h

# Server include must come before client include, because there could
# be mismatching libpq-dev and postgresql-server-dev installed.
PG_CPPFLAGS = -I$(PQINCSERVER) -I$(PQINC) -DNO_SELECT=$(NO_SELECT)

ifdef VPATH
PG_CPPFLAGS += -I$(VPATH)/src
endif

DISTNAME = $(EXTENSION)-$(DISTVERSION)

# regression testing setup
REGRESS = plproxy_init plproxy_test plproxy_select plproxy_many \
     plproxy_errors plproxy_clustermap plproxy_dynamic_record \
     plproxy_encoding plproxy_split plproxy_target
REGRESS_OPTS = --dbname=regression --inputdir=test
# pg9.1 ignores --dbname
override CONTRIB_TESTDB := regression

# sql source
PLPROXY_SQL = sql/plproxy_lang.sql
# Generated SQL files
EXTSQL = sql/$(EXTENSION)--$(EXTVERSION).sql
# Fixed SQL
EXTMISC = sql/plproxy--unpackaged--2.3.0.sql

# PostgreSQL version
PGVER = $(shell $(PG_CONFIG) --version | sed 's/PostgreSQL //')
SQLMED = $(shell test $(PGVER) "<" "8.4" && echo "false" || echo "true")
PG91 = $(shell test $(PGVER) "<" "9.1" && echo "false" || echo "true")

# SQL/MED available, add foreign data wrapper and regression tests
ifeq ($(SQLMED), true)
REGRESS += plproxy_sqlmed
PLPROXY_SQL += sql/plproxy_fdw.sql
endif

# SQL for extensions or plain?
ifeq ($(PG91),true)
DATA_built = $(EXTSQL)
DATA = $(EXTMISC)
EXTRA_CLEAN += sql/plproxy.sql
else
DATA_built = sql/plproxy.sql
EXTRA_CLEAN += $(EXTSQL)
endif


#
# load PGXS makefile
#
PGXS = $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

ifeq ($(PORTNAME), win32)
SHLIB_LINK += -lws2_32 -lpgport
endif

# PGXS may define them as empty
FLEX := $(if $(FLEX),$(FLEX),flex)
BISON := $(if $(BISON),$(BISON),bison)

# parser rules
src/scanner.o: src/parser.tab.h
src/parser.tab.h: src/parser.tab.c

src/parser.tab.c: src/parser.y
	@mkdir -p src
	$(BISON) -b src/parser -d $<

src/scanner.c: src/scanner.l
	@mkdir -p src
	$(FLEX) -o$@ $<

sql/plproxy.sql: $(PLPROXY_SQL)
	@mkdir -p sql
	cat $^ > $@

# plain plproxy.sql is not installed, but used in tests
$(EXTSQL): $(PLPROXY_SQL)
	@mkdir -p sql
	echo "create extension plproxy;" > sql/plproxy.sql 
	cat $^ > $@

# dependencies

$(OBJS): $(HDRS)

# utility rules

tags: $(SRCS) $(HDRS)
	ctags $(SRCS) $(HDRS)

tgz:
	git archive --prefix=$(DISTNAME)/ HEAD | gzip -9 > $(DISTNAME).tar.gz

zip:
	git archive -o $(DISTNAME).zip --format zip --prefix=$(DISTNAME)/ HEAD

clean: doc-clean

doc-clean:
	$(MAKE) -C doc clean

test: install
	$(MAKE) installcheck || { filterdiff --format=unified regression.diffs | less; exit 1; }

ack:
	cp results/*.out test/expected/

deb:
	debuild -uc -us -b

orig:
	make -f debian/rules orig

