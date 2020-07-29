
\c test_part0


create or replace function test_ret_table(id int)
returns table(id int, t text) as $$
    select * from (values(1, 'test'),(2, 'toto') ) as toto;
$$ language sql;

select * from test_ret_table(0);

\c regression

create or replace function test_ret_table_normal(in _id integer, out id integer, out t text)
returns setof record as $$
    cluster 'testcluster';
    run on _id;
    target test_ret_table;
$$ language plproxy;

select * from test_ret_table_normal(0);

create or replace function test_ret_table(in _id integer)
returns table (id integer, t text) as $$
    cluster 'testcluster';
    run on _id;
$$ language plproxy;

select * from test_ret_table(0);

-- test table result set mapping

\c test_part0

CREATE FUNCTION test_result_map()
   RETURNS TABLE (a integer, b text) AS
$$SELECT a, b FROM (VALUES (1, 'one'), (2, 'two')) AS v(a, b)$$
LANGUAGE sql;

\c test_part1

CREATE FUNCTION test_result_map()
   RETURNS TABLE (b text, a integer) AS
$$SELECT b, a FROM (VALUES (3, 'three'), (4, 'four')) AS v(a, b)$$
LANGUAGE sql;

\c test_part2

CREATE FUNCTION test_result_map()
   RETURNS TABLE (a integer, b text) AS
$$SELECT a, b FROM (VALUES (5, 'five'), (6, 'six')) AS v(a, b)$$
LANGUAGE sql;

\c test_part3

CREATE FUNCTION test_result_map()
   RETURNS TABLE (b text, a integer) AS
$$SELECT b, a FROM (VALUES (7, 'seven'), (8, 'eight')) AS v(a, b)$$
LANGUAGE sql;

\c regression

CREATE FUNCTION test_result_map()
   RETURNS TABLE (a integer, b text) AS
$$CLUSTER 'testcluster';
RUN ON ALL;$$
LANGUAGE plproxy;

SELECT * FROM test_result_map() ORDER BY a, b;
