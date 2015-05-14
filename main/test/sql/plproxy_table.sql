
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

