\c regression

create or replace function plproxy.get_cluster_partitions(cluster_name text)
returns setof text as $$
begin
    if cluster_name = 'testcluster' then
        return next 'host=127.0.0.1 dbname=test_part0';
        return next 'host=127.0.0.1 dbname=test_part1';
        return next 'host=127.0.0.1 dbname=test_part2';
        return next 'host=127.0.0.1 dbname=test_part3';
        return;
    end if;
    raise exception 'no such cluster: %', cluster_name;
end; $$ language plpgsql;

CREATE TABLE my_table (dbname text, i int);

-- same query on all nodes
CREATE OR REPLACE FUNCTION test_execute_single_on_all(query text) RETURNS SETOF my_table AS $$
cluster 'testcluster';
run on all;
execute query;
$$ LANGUAGE plproxy;

SELECT * FROM test_execute_single_on_all('SELECT current_database() AS dbname, 1 AS i') ORDER BY dbname, i;
-- Null means nothing gets executed
SELECT * FROM test_execute_single_on_all(NULL) ORDER BY dbname;

-- Wrong datatype is validated
CREATE OR REPLACE FUNCTION test_execute_wrong_datatype(query int) RETURNS SETOF my_table AS $$
cluster 'testcluster';
execute query;
$$ LANGUAGE plproxy;

CREATE OR REPLACE FUNCTION test_execute_wrong_datatype(query int[]) RETURNS SETOF my_table AS $$
cluster 'testcluster';
execute query;
$$ LANGUAGE plproxy;

-- Same query on specific nodes
CREATE OR REPLACE FUNCTION test_execute_single_on_some(nodes int[], query text) RETURNS SETOF my_table AS $$
cluster 'testcluster';
split nodes;
run on nodes;
execute query;
$$ LANGUAGE plproxy;

SELECT * FROM test_execute_single_on_some(array[0,2], 'SELECT current_database() AS dbname, 1 AS i') ORDER BY dbname, i;

-- All queries on all nodes. Maximum of 1 query for now.
CREATE OR REPLACE FUNCTION test_execute_multiple_on_all(queries text[]) RETURNS SETOF my_table AS $$
cluster 'testcluster';
split queries;
run on all;
execute queries;
$$ LANGUAGE plproxy;

SELECT * FROM test_execute_multiple_on_all(array[
	'SELECT current_database() AS dbname, 1 AS i',
	'SELECT current_database() AS dbname, 2 AS i'
]);
SELECT * FROM test_execute_multiple_on_all(array[
	'SELECT current_database() AS dbname, 1 AS i',
	NULL
]) ORDER BY dbname, i;

-- Specify one query per node
CREATE OR REPLACE FUNCTION test_execute_multiple_on_specific(queries text[], nodes int[]) RETURNS SETOF my_table AS $$
cluster 'testcluster';
split nodes, queries;
run on nodes;
execute queries;
$$ LANGUAGE plproxy;

SELECT * FROM test_execute_multiple_on_specific(array[
	'SELECT current_database() AS dbname, 1 AS i',
	'SELECT current_database() AS dbname, 2 AS i'
], array[3,0]) ORDER BY dbname, i;
-- Null queries get skipped 
SELECT * FROM test_execute_multiple_on_specific(array[
	NULL,
	'SELECT current_database() AS dbname, 1 AS i',
	'SELECT current_database() AS dbname, 2 AS i',
	NULL
], array[0,1,2,3]) ORDER BY dbname, i;

-- Array of queries not split is validated
CREATE OR REPLACE FUNCTION test_execute_multiple_no_split(queries text[], nodes int[]) RETURNS SETOF my_table AS $$
cluster 'testcluster';
split nodes;
run on nodes;
execute queries;
$$ LANGUAGE plproxy;

-- Split queries on single node works when only one query
CREATE OR REPLACE FUNCTION test_execute_multiple_on_single(queries text[], node int) RETURNS SETOF my_table AS $$
cluster 'testcluster';
split queries;
run on node;
execute queries;
$$ LANGUAGE plproxy;

SELECT * FROM test_execute_multiple_on_single(array[
	'SELECT current_database() AS dbname, 1 AS i',
	'SELECT current_database() AS dbname, 2 AS i'
], 3);

SELECT * FROM test_execute_multiple_on_single(array[
	'SELECT current_database() AS dbname, 1 AS i',
	NULL
], 3);

-- No queries means no results
SELECT * FROM test_execute_multiple_on_single(array[]::text[], 3);

-- Run one query on specific node
CREATE OR REPLACE FUNCTION test_execute_single_on_single(node int, query text) RETURNS SETOF my_table AS $$
cluster 'testcluster';
run on node;
execute query;
$$ LANGUAGE plproxy;

SELECT * FROM test_execute_single_on_single(0, 'SELECT current_database() AS dbname, 1 AS i');
