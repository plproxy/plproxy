
# PL/Proxy version
PLPROXY_VERSION = 2.0.2

# libpq config
PQINC = $(shell pg_config --includedir)
PQLIB = $(shell pg_config --libdir)

FLEX = flex
BISON = bison 

# module setup
MODULE_big = plproxy
SRCS = src/cluster.c src/execute.c src/function.c src/main.c \
       src/query.c src/result.c src/type.c
OBJS = src/scanner.o src/parser.tab.o $(SRCS:.c=.o)
DATA_built = plproxy.sql
EXTRA_CLEAN = src/scanner.[ch] src/parser.tab.[ch]
PG_CPPFLAGS = -I$(PQINC)
SHLIB_LINK = -L$(PQLIB) -lpq

DIST_FILES = Makefile src/plproxy.h src/rowstamp.h src/scanner.l src/parser.y \
	     sql/*.sql expected/*.out config/*.sql doc/*.txt doc/Makefile \
	     AUTHORS COPYRIGHT README plproxy.sql.in NEWS debian/packages \
		 src/dbgmalloc.h
DIST_DIRS = src sql expected config doc debian
TARNAME = plproxy-$(PLPROXY_VERSION)

# regression testing setup
REGRESS = plproxy_init plproxy_test plproxy_select plproxy_many \
	  plproxy_errors plproxy_clustermap
REGRESS_OPTS = --load-language=plpgsql

# load PGXS makefile
PGXS = $(shell pg_config --pgxs)
include $(PGXS)

# parser rules
src/scanner.o: src/parser.tab.h
src/parser.tab.h: src/parser.tab.c

src/parser.tab.c: src/parser.y
	cd src; $(BISON) -d parser.y

src/scanner.c: src/scanner.l
	cd src; $(FLEX) -oscanner.c scanner.l

# dependencies
$(OBJS): src/plproxy.h src/rowstamp.h src/dbgmalloc.h

# utility rules

tags:
	cscope -I src -b -f .cscope.out src/*.c

tgz:
	rm -rf $(TARNAME)
	mkdir -p $(TARNAME)
	tar c $(DIST_FILES) $(SRCS) | tar x -C $(TARNAME)
	tar czf $(TARNAME).tar.gz $(TARNAME)

clean: tgzclean

tgzclean:
	rm -rf $(TARNAME) $(TARNAME).tar.gz

test: install
	make installcheck || { less regression.diffs; exit 1; }

deb:
	(stamp=`date -R 2>/dev/null` || stamp=`gdate -R`; \
		echo "plproxy2 ($(PLPROXY_VERSION)) unstable; urgency=low"; \
		echo ""; echo "  * New build"; echo ""; \
		echo " -- BuildDaemon <dev@null>  $$stamp") > debian/changelog
	yada rebuild
	debuild -uc -us -b

mainteiner-clean: clean
	rm -f src/scanner.[ch] src/parser.tab.[ch]

