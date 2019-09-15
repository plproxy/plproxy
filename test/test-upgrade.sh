#! /bin/sh

set -e

dbname=plproxyupgrade

upgrade() {
    first="$1"
    second="$2"
    dropdb ${dbname} || true
    make -s -C $first clean install
    createdb ${dbname}
    psql -d $dbname -c 'create extension plproxy;'
    make -s -C $second clean install
    psql -d $dbname -c 'alter extension plproxy update;'
}

upgrade plproxy-2.6 plproxy-2.9
upgrade plproxy-2.7 plproxy-2.9
upgrade plproxy-2.8 plproxy-2.9

