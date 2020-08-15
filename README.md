
# PL/Proxy

PL/Proxy is a PostgreSQL procedural language (PL) handler that allows to do
remote procedure calls between PostgreSQL databases, with optional sharding.

## Features

Language has four statements:

* Pick remote database:
  * `CLUSTER <name>` - use pre-configured cluster that has many databases
  * `CONNECT <connstr>` - use connstr directly
* Set execution type:
  * `RUN ON ALL` - query is run on all databases in parallel
  * `RUN ON ANY` - pick server randomly
  * `RUN ON <hash>` - map hash to database
* Replace default query:
  * `SELECT ...`

Example:

```sql
CREATE FUNCTION get_user_settings(i_username text) RETURNS SETOF user_settings AS $$
    RUN ON namehash(i_username):
$$ LANGUAGE plproxy;
```

It will run function with same name in remote database and fetch `user_settings` record.

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

