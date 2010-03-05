
# PL/Proxy version
PLPROXY_VERSION = 2.1-cvs

# libpq config
PQINC = $(shell pg_config --includedir)
PQLIB = $(shell pg_config --libdir)

# PostgreSQL version
PGVER = $(shell pg_config --version | sed 's/PostgreSQL //')
SQLMED = $(shell test $(PGVER) "<" "8.4" && echo "false" || echo "true")

# module setup
MODULE_big = plproxy
SRCS = src/cluster.c src/execute.c src/function.c src/main.c \
       src/query.c src/result.c src/type.c src/poll_compat.c
OBJS = src/scanner.o src/parser.tab.o $(SRCS:.c=.o)
DATA_built = plproxy.sql
EXTRA_CLEAN = src/scanner.[ch] src/parser.tab.[ch] plproxy.sql.in
PG_CPPFLAGS = -I$(PQINC)
SHLIB_LINK = -L$(PQLIB) -lpq

TARNAME = plproxy-$(PLPROXY_VERSION)
DIST_DIRS = src sql expected config doc debian
DIST_FILES = Makefile src/plproxy.h src/rowstamp.h src/scanner.l src/parser.y \
			 $(foreach t,$(REGRESS),sql/$(t).sql expected/$(t).out) \
			 config/simple.config.sql src/poll_compat.h \
			 doc/Makefile doc/config.txt doc/overview.txt doc/faq.txt \
			 doc/syntax.txt doc/todo.txt doc/tutorial.txt \
			 AUTHORS COPYRIGHT README plproxy_lang.sql plproxy_fdw.sql NEWS \
			 debian/packages debian/changelog

# regression testing setup
REGRESS = plproxy_init plproxy_test plproxy_select plproxy_many \
	  plproxy_errors plproxy_clustermap plproxy_dynamic_record \
	  plproxy_encoding plproxy_split

# SQL files
PLPROXY_SQL = plproxy_lang.sql

# SQL/MED available, add foreign data wrapper and regression tests
ifeq ($(SQLMED), true)
REGRESS += plproxy_sqlmed
PLPROXY_SQL += plproxy_fdw.sql
endif

REGRESS_OPTS = --dbname=regression

# load PGXS makefile
PGXS = $(shell pg_config --pgxs)
include $(PGXS)

ifeq ($(PORTNAME), win32)
SHLIB_LINK += -lws2_32 -lpgport
endif

# PGXS may define them as empty
FLEX := $(or $(FLEX), flex)
BISON := $(or $(BISON), bison)

# parser rules
src/scanner.o: src/parser.tab.h
src/parser.tab.h: src/parser.tab.c

src/parser.tab.c: src/parser.y
	cd src; $(BISON) -d parser.y

src/scanner.c: src/scanner.l
	cd src; $(FLEX) -oscanner.c scanner.l

plproxy.sql.in: $(PLPROXY_SQL)
	cat $^ > $@

# dependencies
$(OBJS): src/plproxy.h src/rowstamp.h
src/execute.o: src/poll_compat.h
src/poll_compat.o: src/poll_compat.h

# utility rules

tags:
	cscope -I src -b -f .cscope.out src/*.c

tgz:
	rm -rf $(TARNAME)
	mkdir -p $(TARNAME)
	tar c $(DIST_FILES) $(SRCS) | tar xp -C $(TARNAME)
	tar czf $(TARNAME).tar.gz $(TARNAME)

clean: tgzclean

tgzclean:
	rm -rf $(TARNAME) $(TARNAME).tar.gz

test: install
	make installcheck || { less regression.diffs; exit 1; }

ack:
	cp results/*.out expected/

mainteiner-clean: clean
	rm -f src/scanner.[ch] src/parser.tab.[ch]

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

