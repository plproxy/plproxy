#!/bin/bash -xue

export TESTDIR="$(pwd)/regressiondata"
export PGPORT=$((10240 + RANDOM / 2))
export PGDATA="$TESTDIR/pg"
rm -rf "$TESTDIR"
mkdir -p "$PGDATA"
initdb -E UTF-8 --no-locale --nosync
sed -e "s%^#port =.*%port = $PGPORT%" \
    -e "s%^#\(unix_socket_director[a-z]*\) =.*%\1 = '$PGDATA'%" \
    -e "s%^#dynamic_library_path = .*%dynamic_library_path = '$(pwd):\$libdir'%" \
    -e "s%^#fsync = .*%fsync = off%" \
    -e "s%^#synchronous_commit = .*%synchronous_commit = off%" \
    -i "$PGDATA/postgresql.conf"
pg_ctl -l "$PGDATA/log" start
while [ ! -S "$PGDATA/.s.PGSQL.$PGPORT" ]; do sleep 2; done
trap "pg_ctl stop -m immediate" EXIT

cp -a "test/" "sql/" "$TESTDIR/"
echo "\\i sql/plproxy--$EXTVERSION.sql" > "$TESTDIR/sql/plproxy.sql"
find "$TESTDIR/test" -type f -print0 | xargs -0r \
	sed -e "s%host=.*dbname=%host=$PGDATA port=$PGPORT dbname=%" -i
cd "$TESTDIR"
/usr/lib64/pgsql/pgxs/src/makefiles/../../src/test/regress/pg_regress \
    --host="$PGDATA" --port="$PGPORT" \
    --inputdir=. --psqldir='/usr/bin' \
    --dbname=regression --inputdir=test --dbname=regression \
    plproxy_init plproxy_test plproxy_select plproxy_many plproxy_errors plproxy_clustermap plproxy_dynamic_record plproxy_encoding plproxy_split plproxy_target plproxy_alter plproxy_cancel plproxy_sqlmed plproxy_table plproxy_range
