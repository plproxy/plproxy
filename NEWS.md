
# PL/Proxy Changelog

**2017-10-08  -  PL/Proxy 2.8  -  "Entropy Always Wins"**

- Fixes:

  * PG10: Support more than one digit versions in Makefile.
    (Luis Bosque)

  * PG10: Use TupleDescAttr() to handle changed type in tupdesc.

  * Use corrent <sys/socket.h> detection macro.
    (Christoph Moench-Tegeder)

**2016-12-27  -  PL/Proxy 2.7  -  "Never Trust Sober Santa"**

- Fixes
  * Update to newer `heap_form_tuple()` API.
    (Peter Eisentraut)

  * Handle 64-bit `SPI_processed`.
    (Peter Eisentraut)

**2015-08-26  -  PL/Proxy 2.6  -  "Released via Bottle Mail"**

- Features

  * Language validator.
    (Peter Eisentraut)

  * Support Postgres 9.5.
    (Peter Eisentraut)

- Fixes

  * Query cancel fixes.
    (Fazal Majid, Tarvi Pillessaar)

  * Fix `yy_scan_bytes()` prototype.
    (Peter Eisentraut)

  * Use a proper sqlstate with non-setof function errors.
    (Oskari Saarenmaa)

- Cleanups

  * Convert docs to markdown.

  * Debian packaging cleanup.

**2012-11-27  -  PL/Proxy 2.5  -  "With Extra Leg For Additional Stability"**

- Features

  * Support `RETURNS TABLE` syntax.

  * Support range types.

  * Make it build against 9.3dev.  (Peter Eisentraut)

- Fixes

  * When sending cancel request to server, wait for answer
    and discard it.  Without waiting the local backend might
    be killed and cancel request dropped before it reaches
    remote server - eg. when there is pgbouncer in the middle.

  * When return record is based on type or table, detect changes
    and reload the type.

  * Allow type or table to have dropped fields.

  * Fix crash when `RETURNS TABLE` syntax is used.

**2012-05-07  -  PL/Proxy 2.4  -  "Light Eater"**

- Features

  * Use `current_user` as default user for remote connections.
    Allow access with different users to same cluster in same backend.
    Old default was `session_user`, but that does not seem useful anymore.

  * Support ENUM types.  (Zoltán Böszörményi)

  * Support building as Postgres extension on 9.1+.  (David E. Wheeler)

  * Support building as [PGXN](http://pgxn.org) extension.  (David E. Wheeler)

  * Support Postgres 9.2.


**2011-10-25  -  PL/Proxy 2.3  -  "Unmanned Crowd Control"**

- Features

  * Global SQL/MED options: `ALTER FOREIGN DATA WRAPPER plproxy OPTIONS ... ;`
    (Petr Jelinek)

  * New config options: `keepalive_idle`, `keepalive_interval`, `keepalive_count`.
    For TCP keepalive tuning.  Alternative to libpq keepalive options
    when older libpq is used or when central tuning is preferrable.

- Fixes

  * Fix memory leak in SPLIT - it was leaking array element type info.

- Doc

  * Use Asciidoc ListingBlock for code - results in nicer HTML.

**2011-02-18  -  PL/Proxy 2.2  -  "Cover With Milk To See Secret Message"**

- Features

  * New TARGET statement to specify different function to call
    on remote side.

  * Make possible to compile out support for SELECT statement:

        $ make all NO_SELECT=1

- Fixes

  * Fix returning of many-column (>100) result rows.  Old code assumed
    FUNC_MAX_ARGS is max, but that does not apply to result columns.
    (Hans-Jürgen Schönig)

  * Survive missing sqlstate field on error messages.
    Local libpq errors do not set it.

  * More portable workaround for empty FLEX/BISON.
    (Peter Eisentraut)

  * win32: Fix poll compat to work with large amount of fds.
    Old compat assument bitmap representation for fd_set,
    but win32 uses array.

**2010-04-23  -  PL/Proxy 2.1  -  "Quality Horrorshow"**

- Features

  * SPLIT: New `SPLIT` statement to convert incoming array arguments
    into smaller per-partition arrays.

    (Martin Pihlak)

  * SQL/MED: Cluster can be defined with SQL/MED facilities,
    instead of old-style plproxy.* functions.

    (Martin Pihlak)

- Minor fixes/features

  * Allow to customize location to `pg_config` via `PG_CONFIG` make variable.
    (David E. Wheeler)

  * Remote errors and notices are now passed upwards with all details.
    Previously only error message pas passed and notices were ignored.

  * Show remote database name in error messages.

  * Compatible with Postgres 9.0.

  * Compatible with flex 2.5.35+ - it now properly defines it's
    own functions, so PL/Proxy does not need to do it.  Otherwise
    compilation will fail if flex definitions are hacked (MacOS).

  * Rework regests to make them work across 8.2..9.0 and decrease
    chance of spurious failures.  The encoding test still fails
    if Postgres instance is not created with LANG=C.

  * deb: per-version packaging: `make debXY` will create
    `postgresql-plproxy-X.Y` package.

**2009-10-28  -  PL/Proxy 2.0.9  -  "Five-Nines Guarantee For Not Bricking The Server"**

- Features

  * More flexible CONNECT statement.

        CONNECT func(..);
        CONNECT argname;
        CONNECT $argnum;

    NB: giving untrusted users ability to specify full connect string creates
    security hole.  Eg it can used to read cleartext passwords from .pgpass/pg_service.
    If such function cannot be avoided, it's access rights need to be restricted.

    (Ian Sollars)

- Fixes

  * Avoid parsing `SELECT (` as function call.  Otherwise following query
    fails to parse: `SELECT (0*0);`
    (Peter Eisentraut)

  * Make scanner accept dot as standalone symbol.  Otherwise following query
    fails to parse: `SELECT (ret_numtuple(1)).num, (ret_numtuple(1)).name;`
    (Peter Eisentraut)

  * Argument type name length over 32 could cause buffer overflow.
    (Ian Sollars)

  * Fix crash with incoming NULL value in function containing SELECT
    with different argument order.  Due to thinko, NULL check was done
    with query arg index, instead of function arg index.
    (João Matos)

  * geterrcode(): Switch memory context to work around Assert() in CopyErrorData().

**2009-01-16  -  PL/Proxy 2.0.8  -  "Simple Multi-Tentacle Arhitecture"**

- Features

  * If query is canceled, send cancel request to remote db too.
    (Ye Wenbin)

  * Allow direct argument references in RUN ON statement.
    Now this works: `RUN ON $1;` and  `RUN ON arg;`

  * Add FAQ to docs which answers few common questions.

- Fixes

  * Clear `->tuning` bit on connection close.

    If `SET client_encoding` fails, the bit can stay set,
    thus creating always-failing connection slot.

    Reported and analyzed by Jonah Harris.

**2008-09-29  -  PL/Proxy 2.0.7  -  "The Ninja of Shadow"**

- Fixes

  * Make sure `client_encoding` on remote server encoding is set to
    local server encoding.  Currently plproxy set it to local
    `client_encoding`, which is wrong as all data is immediately converted
    to `server_encoding` by Postgres.  The problem went mostly undetected
    because of plproxy use of binary I/O which bypasses encoding
    conversions.
    (Hiroshi Saito)

    So if you pass non-ascii data around and your client, proxy server
    and target server may have different encodings, you may want to
    re-check your data.

  * Disable binary i/o completely.  Currently the decision is done
    too early, before remote connection is established.  Currently
    the fix was to use only "safe" types for binary I/O, but now
    that text types are also unsafe, it's pointless.  Instead
    type handling should be rewritten to allow lazy decision-making.

  * Fix crash with unnamed function input arguments.

  * Fix compilation with 8.2 on Win32 by providing PGDLLIMPORT if unset.
    (Hiroshi Saito)

  * Accept >128 chars as part of identifier names.

  * New regtest to detect encoding problems.

  * Use `pg_strcasecmp()` instead of `strcasecmp()` to be consistent with
    Postgres code.

  * deb: Survive empty FLEX/BISON defs from PGXS.
    Accept also postgresql-server-dev-8.3 as build dep.

**2008-09-05  -  PL/Proxy 2.0.6  -  "Agile Voodoo"**

- Features

  * Support functions that return plain RECORD without
    OUT parameters.  Such functions need result type
    specified on each call with AS clause and the
    types need to be sent to remote database also.
    (Lei Yonghua)

    This makes possible to use PL/Proxy for dynamic queries:

    CREATE FUNCTION run_query(sql text) RETURNS RECORD ..
    SELECT * FROM run_query('select a,b from ..') AS (a int, b text);

  * Accept int2/int8 values from hash function,
    in addition to int4.

- Fixes

  * Replace bitfields with bool to conform better
    with Postgres coding style.
  * Don't use alloca() in parser.
  * Make scanner more robust to allocation errors
    by doing total reset before parsing.
  * Require exactly one row from remote query for
    non-SETOF functions.
  * Docs: tag all functions with embedded SELECT with SETOF.
  * Make it compile on win32 (Hiroshi Saito)
  * Make regtest tolerant to random() implementation differneces
    between different OSes.

**2008-06-06  -  PL/Proxy 2.0.5  -  "Universal Shredder"**

- Fixes:

  * Fix crash if a function with `CLUSTER 'name';`
    is parsed after function with `CLUSTER func();`.
    A palloc()ed pointer was not reset.
  * `RUN ON` is now optional, defaulting to `RUN ON ANY;`
    Should make creating simple RPC and load-balancing
    functions easier easier.
  * Make compat poll() function behave more like actual poll().

**2008-01-04  -  PL/Proxy 2.0.4  -  "Vampire-proof"**

- Fixes

  * Fix crash due to bad error reporting when remote db
    closes socket unexpectedly.
  * Use `pg_strcasecmp()` to compare encodings.
  * Log encoding values if it fails to apply.
  * Replace select(2) with poll(2) to allow large fd values.
    Old select(2) usage could cause problems when plproxy
    was installed on database with lot of tables/indexes,
    where Postgres itself could have had lot of files open.
  * Disable binary I/O for timestamp(tz) and date/time types,
    to avoid problems when local and remote Postgres have
    different setting for integer_datetimes.

**2007-12-10  -  PL/Proxy 2.0.3  -  "Faster Than A Fresh Zombie"**

- Features

  * Explicitly specify result column names and types in query.

    Lets say there is function somefunc(out id int4, out data text).
    Previously pl/proxy issued following query:

        SELECT * FROM somefunc()

    And later tried to work out which column goes where.  Now it issues:

        SELECT id::int4, data::text FROM somefunc()

    For functions without named return paramenters, eg. just "RETURNS text":

        SELECT r::text FROM anotherfunc() r

    This gives better type safety when using binary I/O, allows signatures
    differ in deterministic ways and creates safe upgrade path for signatures.

    Only downside is that existing functions with wildly different signatures
    stop working, but as they work on pure luck anyway, I'm not worried.

  * Quote function and result column names properly.

  * Set `client_encoding` on remote database to be equal to local one.

  * Tutorial by Steve Singer.

- Fixes

  * Support 8.3 (handle short varlena header)

  * Support old flex (2.5.4)  Previously flex >= 2.5.33 was required.

  * Fix `make deb`, include actual debian/changelog.

  * Remove config paramenter `statement_timeout`.

    It was ignored previously and it cannot be made work in live env
    when working thru pgbouncer, so its better to drop it completely.
    The setting can be always set via normal ways.

**2007-04-16  -  PL/Proxy 2.0.2  -  "No news is good news?"**

- Cleanups

  * Include plproxy.sql.in in tgz.

  * Clean `add_connection()` function by using StringInfo instead
    open-coded string shuffling.

**2007-03-30  -  PL/Proxy 2.0.1 - "PL/Proxy 2.0.1"**

- Fixes

  * Support for 8.3

  * Seems v2.0 invalidated cache more than intended. Fix.

**2007-03-13  -  PL/Proxy 2.0 - "Skype Presents"**

- Initial
  [public release](http://www.postgresql.org/message-id/54335.194.126.108.9.1173801315.squirrel@mail.skype.net)
  of v2.0 rewrite.

