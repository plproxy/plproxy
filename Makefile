EXTENSION  = plproxy
EXTVERSION = $(shell grep default_version $(EXTENSION).control | \
             sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")

# set to 1 to disallow functions containing SELECT
NO_SELECT = 0

# libpq config
PG_CONFIG = pg_config
PQINC = $(shell $(PG_CONFIG) --includedir)
PQLIB = $(shell $(PG_CONFIG) --libdir)

# PostgreSQL version
PGVER = $(shell $(PG_CONFIG) --version | sed 's/PostgreSQL //')
SQLMED = $(shell test $(PGVER) "<" "8.4" && echo "false" || echo "true")
PG91 = $(shell test $(PGVER) "<" "9.1" && echo "false" || echo "true")

# module setup
MODULE_big = plproxy
SRCS = src/cluster.c src/execute.c src/function.c src/main.c \
       src/query.c src/result.c src/type.c src/poll_compat.c
OBJS = src/scanner.o src/parser.tab.o $(SRCS:.c=.o)
DATA_built = sql/plproxy.sql
EXTRA_CLEAN = src/scanner.[ch] src/parser.tab.[ch] sql/plproxy.sql
PG_CPPFLAGS = -I$(PQINC) -DNO_SELECT=$(NO_SELECT)
SHLIB_LINK = -L$(PQLIB) -lpq

DISTNAME = plproxy-$(EXTVERSION)

# regression testing setup
REGRESS = plproxy_init plproxy_test plproxy_select plproxy_many \
     plproxy_errors plproxy_clustermap plproxy_dynamic_record \
     plproxy_encoding plproxy_split plproxy_target

# SQL files
PLPROXY_SQL = sql/plproxy_lang.sql

# SQL/MED available, add foreign data wrapper and regression tests
ifeq ($(SQLMED), true)
REGRESS += plproxy_sqlmed
PLPROXY_SQL += sql/plproxy_fdw.sql
endif

# Extensions available, rename files as appropriate.
ifeq ($(PG91),true)
all: sql/$(EXTENSION)--$(EXTVERSION).sql

sql/$(EXTENSION)--$(EXTVERSION).sql: sql/$(EXTENSION).sql
	cp $< $@

DATA = $(wildcard sql/*--*.sql) sql/$(EXTENSION)--$(EXTVERSION).sql
EXTRA_CLEAN += sql/$(EXTENSION)--$(EXTVERSION).sql
endif


REGRESS_OPTS = --dbname=regression --inputdir=test

# pg9.1 ignores --dbname
override CONTRIB_TESTDB := regression

# load PGXS makefile
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
	cd src; $(BISON) -d parser.y

src/scanner.c: src/scanner.l
	cd src; $(FLEX) -oscanner.c scanner.l

sql/plproxy.sql: $(PLPROXY_SQL)
	cat $^ > $@

# dependencies
$(OBJS): src/plproxy.h src/rowstamp.h
src/execute.o: src/poll_compat.h
src/poll_compat.o: src/poll_compat.h

# utility rules

tags:
	cscope -I src -b -f .cscope.out src/*.c

tgz:
	git archive -o $(DISTNAME).tar.gz --prefix=$(DISTNAME)/ HEAD

zip:
	git archive -o $(DISTNAME).zip --format zip --prefix=$(DISTNAME)/ HEAD

clean: doc-clean

doc-clean:
	$(MAKE) -C doc clean

test: install
	$(MAKE) installcheck || { less regression.diffs; exit 1; }

ack:
	cp results/*.out expected/

maintainer-clean: clean
	rm -f src/scanner.[ch] src/parser.tab.[ch]
	rm -rf debian/control debian/rules debian/packages debian/packages-tmp*

deb82:
	sed -e s/PGVER/8.2/g < debian/packages.in > debian/packages
	yada rebuild
	debuild -uc -us -b

deb83:
	sed -e s/PGVER/8.3/g < debian/packages.in > debian/packages
	yada rebuild
	debuild -uc -us -b

deb84:
	sed -e s/PGVER/8.4/g < debian/packages.in > debian/packages
	yada rebuild
	debuild -uc -us -b

deb90:
	sed -e s/PGVER/9.0/g < debian/packages.in > debian/packages
	yada rebuild
	debuild -uc -us -b

deb91:
	sed -e s/PGVER/9.1/g < debian/packages.in > debian/packages
	yada rebuild
	debuild -uc -us -b

