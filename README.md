
# PL/Proxy

Sven Suursoho & Marko Kreen

## Installation

For installation there must be PostgreSQL dev environment installed
and `pg_config` in the PATH.   Then just run:

    $ make
    $ make install

To run regression tests:

    $ make installcheck

Location to `pg_config` can be set via `PG_CONFIG` variable:

    $ make PG_CONFIG=/path/to/pg_config
    $ make install PG_CONFIG=/path/to/pg_config
    $ make installcheck PG_CONFIG=/path/to/pg_config

Note: Encoding regression test fails if the Postgres instance is not created with C locale.
It can be considered expected failure then.

