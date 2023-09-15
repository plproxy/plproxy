EXTENSION  = plproxy

# sync with NEWS, META.json, plproxy.control
EXTVERSION = 2.10.0
UPGRADE_VERS = 2.3.0 2.4.0 2.5.0 2.6.0 2.7.0 2.8.0 2.9.0
DISTVERSION = $(EXTVERSION)

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
       src/query.c src/result.c src/type.c src/aatree.c
OBJS = src/scanner.o src/parser.tab.o $(SRCS:.c=.o)
EXTRA_CLEAN = src/scanner.[ch] src/parser.tab.[ch] libplproxy.* plproxy.so
SHLIB_LINK = -L$(PQLIB) -lpq

HDRS = src/plproxy.h src/rowstamp.h src/aatree.h

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
     plproxy_encoding plproxy_split plproxy_target plproxy_alter \
     plproxy_cancel plproxy_range plproxy_sqlmed plproxy_table \
     plproxy_modular plproxy_execute
REGRESS_OPTS = --inputdir=test

# use known db name
override CONTRIB_TESTDB := regression

PLPROXY_SQL = sql/plproxy_lang.sql sql/plproxy_fdw.sql
DATA_built = sql/$(EXTENSION)--$(EXTVERSION).sql \
	     $(foreach v,$(UPGRADE_VERS),sql/plproxy--$(v)--$(EXTVERSION).sql)

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

src/parser.o: src/scanner.h
src/scanner.h: src/scanner.c
src/scanner.c: src/scanner.l
	@mkdir -p src
	$(FLEX) -o$@ --header-file=$(@:.c=.h) $<

sql/$(EXTENSION)--$(EXTVERSION).sql: $(PLPROXY_SQL)
	@mkdir -p sql
	cat $^ > $@

$(foreach v,$(UPGRADE_VERS),sql/plproxy--$(v)--$(EXTVERSION).sql): sql/ext_update_validator.sql
	@mkdir -p sql
	cat $< >$@

# dependencies

$(OBJS): $(HDRS)

# utility rules

tags: $(SRCS) $(HDRS)
	ctags $(SRCS) $(HDRS)

dist:
	git archive --prefix=$(DISTNAME)/ HEAD | gzip -9 > $(DISTNAME).tar.gz

zip:
	git archive -o $(DISTNAME).zip --format zip --prefix=$(DISTNAME)/ HEAD

test: install
	$(MAKE) installcheck || { filterdiff --format=unified regression.diffs | less; exit 1; }

citest:
	$(MAKE) installcheck || { filterdiff --format=unified regression.diffs; exit 1; }

ack:
	cp results/*.out test/expected/

checkver:
	@echo "Checking version numbers"
	@test "$(DISTVERSION)" = "$(EXTVERSION)" \
		|| { echo "ERROR: DISTVERSION <> EXTVERSION"; exit 1; }
	@grep -q "^default_version *= *'$(EXTVERSION)'" $(EXTENSION).control \
		|| { echo "ERROR: $(EXTENSION).control has wrong version"; exit 1; }
	@grep -q "Proxy $(EXTVERSION) " NEWS.md \
		|| { echo "ERROR: NEWS.md needs version"; exit 1; }
	@grep '"version"' META.json | head -n 1 | grep -q '"$(EXTVERSION)"' \
		|| { echo "ERROR: META.json has wrong version"; exit 1; }

REPO = github
release: checkver
	git diff --exit-code
	git tag v$(EXTVERSION)
	git push $(REPO)
	git push $(REPO) v$(EXTVERSION):v$(EXTVERSION)

doc/note.md: Makefile NEWS.md
	awk -vVER=$(EXTVERSION) -f doc/note.awk NEWS.md > doc/note.md

