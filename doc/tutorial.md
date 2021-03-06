
# PL/Proxy tutorial

This section explains how to use PL/Proxy to proxy queries across a set of 
remote databases. For purposes of this intro we assume 
each remote database has a "user" table that contains a username and an email 
column.

We also assume that the data is partitioned across remote databases by taking
a hash of the username and assigning it to one of 2 servers. Real applications 
should use a partitioning scheme that is appropriate to their requirements.

For the purposes of this example assume that the partition databases part00
and part01 both contain a table resembling

    CREATE TABLE users (
        username text,
        email text
    );

## Installation

 1. Download PL/Proxy from [https://plproxy.github.com]() and extract.

 2. Build PL/Proxy by running `make` and `make install` inside of the plproxy 
    directory. If your having problems make sure that pg_config from the
    postgresql bin directory is in your path.

 3. To install PL/Proxy in a database execute the commands in the `plproxy.sql` 
    file.  For example `psql -f $SHAREDIR/contrib/plproxy.sql mydatabase`

Steps 1 and 2 can be skipped if your installed pl/proxy from a packaging system
such as RPM.


## Simple remote function call

Here we will create a plproxy function that will run on the proxy database which
will connect to a remote database named
'part00' and return a users email address.

This example  uses plproxy in CONNECT mode, it will 
connect to `dbname=part00` and run following SQL there:

    CREATE FUNCTION get_user_email(i_username text)
    RETURNS SETOF text AS $$
        CONNECT 'dbname=part00';
        SELECT email FROM users WHERE username = $1;
    $$ LANGUAGE plproxy;

    SELECT * from get_user_email($1);

The above example uses plproxy to proxy the query to the remote database but 
doesn't handle partitioning of data.  It assumes that the entire users table is 
in the remote users database.  The next few steps will describe how to partition 
data with PL/Proxy.


## Create  configuration functions

Using PL/Proxy for partitioning requires setting up some configuration functions.
Alternatively, if you are running PostgreSQL 8.4 or above you can take advantage
of the SQL/MED connection management facilities. See below.

When a query needs to be forwarded to a remote database the function
`plproxy.get_cluster_partitions(cluster)` is invoked by plproxy to get the
connection string to use for each partition. 

The following is an example 

    CREATE OR REPLACE FUNCTION plproxy.get_cluster_partitions(cluster_name text)
    RETURNS SETOF text AS $$
    BEGIN
        IF cluster_name = 'usercluster' THEN
            RETURN NEXT 'dbname=part00 host=127.0.0.1';
            RETURN NEXT 'dbname=part01 host=127.0.0.1';
            RETURN;
        END IF;
        RAISE EXCEPTION 'Unknown cluster';
    END;
    $$ LANGUAGE plpgsql;
 
A production application might query some configuration tables to return the
connstrings. The number of partitions must be a power of 2 unless `modular_mapping`
is set.

Next define a `plproxy.get_cluster_version(cluster_name)` function.  This is 
called on each request and determines if the output from a cached
result from `plproxy.get_cluster_partitions()` can be reused. 

    CREATE OR REPLACE FUNCTION plproxy.get_cluster_version(cluster_name text)
    RETURNS int4 AS $$
    BEGIN
        IF cluster_name = 'usercluster' THEN
            RETURN 1;
        END IF;
        RAISE EXCEPTION 'Unknown cluster';
    END;
    $$ LANGUAGE plpgsql;

We also need to provide a `plproxy.get_cluster_config()` function, ours will provide
a value for the connection lifetime.  See the configuration section for details 
on what this function can do. 

    CREATE OR REPLACE FUNCTION plproxy.get_cluster_config(
        in cluster_name text,
        out key text,
        out val text)
    RETURNS SETOF record AS $$
    BEGIN
        -- lets use same config for all clusters
        key := 'connection_lifetime';
        val := 30*60; -- 30m
        RETURN NEXT;
        RETURN;
    END;
    $$ LANGUAGE plpgsql;

The config section contains more information on all of these functions.


## Configuring Pl/Proxy clusters with SQL/MED

First we need a foreign data wrapper. This is mostly a placeholder, but can
be extended with a validator function to verify the cluster definition. See
[CREATE FOREIGN DATA WRAPPER](http://www.postgresql.org/docs/9.4/static/sql-createforeigndatawrapper.html)
documentation for additional details of how to manage the SQL/MED catalog.

    CREATE FOREIGN DATA WRAPPER plproxy;

Then the actual cluster with its configuration options and partitions:

    CREATE SERVER usercluster FOREIGN DATA WRAPPER plproxy
    OPTIONS (connection_lifetime '1800',
             p0 'dbname=part00 host=127.0.0.1',
             p1 'dbname=part01 host=127.0.0.1' );

We also need a user mapping that maps local PostgreSQL users to remote
partitions. It is possible to create PUBLIC mapping that applies for
all users in the local system:

    CREATE USER MAPPING FOR PUBLIC SERVER usercluster;

Or a private mapping that can only be used by specific users:

    CREATE USER MAPPING FOR bob SERVER usercluster 
        OPTIONS (user 'plproxy', password 'salakala');

Finally we need to grant USAGE on the cluster to specific users:

    GRANT USAGE ON SERVER usercluster TO bob;

## Partitioned remote call

Here we assume that the user table is spread over several databases based
on a hash of the username. The connection string for the partitioned databases 
are contained in the `get_cluster_partitions()` function described above.

Below is a `get_user_email()` function that is executed on the proxy server,which
will make a remote connection to the appropriate partitioned database. The
user's email address will be returned.

This function should be created in the proxy database.

    CREATE OR REPLACE FUNCTION get_user_email(i_username text)
    RETURNS SETOF text AS $$
        CLUSTER 'usercluster';
        RUN ON hashtext(i_username) ;
        SELECT email FROM users WHERE username = i_username;
    $$ LANGUAGE plproxy;


## Inserting into the proper partition

Next we provide a simple INSERT function.  

Inserting data through plproxy requires functions to be defined on the proxy 
databases that will perform the insert.

We define this function on both part00 and part01

    CREATE OR REPLACE FUNCTION insert_user(i_username text, i_emailaddress text)
    RETURNS integer AS $$
           INSERT INTO users (username, email) VALUES ($1,$2);
           SELECT 1;
    $$ LANGUAGE SQL;

Now we define a proxy function inside the proxy database to send the 
INSERT's to the appropriate target.

    CREATE OR REPLACE FUNCTION insert_user(i_username text, i_emailaddress text)
    RETURNS integer AS $$
        CLUSTER 'usercluster';
        RUN ON hashtext(i_username);
    $$ LANGUAGE plproxy;

## Putting it all together

Connect to the proxy database (The one we installed plproxy and the plproxy
functions on).

    SELECT insert_user('Sven','sven@somewhere.com');
    SELECT insert_user('Marko', 'marko@somewhere.com');
    SELECT insert_user('Steve','steve@somewhere.cm');

Now connect to the `plproxy_1` and `plproxy_2` databases.  Sven and Marko should be
in `plproxy_2`, and Steve should be in `plproxy_1`.

When connected to the proxy user you can obtain data by doing

    SELECT get_user_email('Sven');
    SELECT get_user_email('Marko');
    SELECT get_user_email('Steve');

## Connection pooling

When used in partitioned setup, PL/Proxy somewhat wastes connections
as it opens connection to each partition from each backend process.
So it's good idea to use a pooler that can take queries from several
connections and funnel them via smaller number of connections to actual
database.  We use and recommend
[PgBouncer](https://pgbouncer.github.io)
for that.


## More resources

Kristo Kaiv has his own take on tutorial here:
[https://kaiv.wordpress.com/category/plproxy]()

