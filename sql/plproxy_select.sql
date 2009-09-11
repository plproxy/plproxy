
-- test regular sql
create function test_select(xuser text, tmp boolean)
returns integer as $x$
    cluster 'testcluster';
    run on hashtext(xuser);
    select /*********
    junk ;
    ********** ****/ id from sel_test where username = xuser
     and ';' <> 'as;d''a ; sd'
    and $tmp$ ; 'a' $tmp$ <> 'as;d''a ; sd'
    and $tmp$ $ $$  $foo$tmp$ <> 'x';
$x$ language plproxy;

\c test_part
create table sel_test (
    id integer,
    username text
);
insert into sel_test values ( 1, 'user');

\c regression
select * from test_select('user', true);
select * from test_select('xuser', false);


-- test errors
create function test_select_err(xuser text, tmp boolean)
returns integer as $$
    cluster 'testcluster';
    run on hashtext(xuser);
    select id from sel_test where username = xuser;
    select id from sel_test where username = xuser;
$$ language plproxy;

select * from test_select_err('user', true);


create function get_zero()
returns setof integer as $x$
    cluster 'testcluster';
    run on all;
    select (0*0);
$x$ language plproxy;

select * from get_zero();

\c test_part
create table numbers (
    num int,
    name text
);
insert into numbers values (1, 'one');
insert into numbers values (2, 'two');

create function ret_numtuple(int)
returns numbers as $x$
    select num, name from numbers where num = $1;
$x$ language sql;

\c regression
create type numbers_type as (num int, name text);

create function get_one()
returns setof numbers_type as $x$
    cluster 'testcluster';
    run on all;
    select (ret_numtuple(1)).num, (ret_numtuple(1)).name;
$x$ language plproxy;

select * from get_one();


\c test_part
create function remote_func(a varchar, b varchar, c varchar)
returns void as $$
begin
    return;
end;
$$ language plpgsql;

\c regression
CREATE OR REPLACE FUNCTION test1(x integer, a varchar, b varchar, c varchar)
RETURNS void AS $$
CLUSTER 'testcluster';
RUN ON 0;
SELECT * FROM remote_func(a, b, c);
$$ LANGUAGE plproxy;

select * from test1(1, 'a', NULL,NULL);
select * from test1(1, NULL, NULL,NULL);

CREATE OR REPLACE FUNCTION test2(a varchar, b varchar, c varchar)
RETURNS void AS $$
CLUSTER 'testcluster';
RUN ON 0;
SELECT * FROM remote_func(a, b, c);
$$ LANGUAGE plproxy;

select * from test2(NULL, NULL, NULL);
select * from test2('a', NULL, NULL);


CREATE OR REPLACE FUNCTION test3(a varchar, b varchar, c varchar)
RETURNS void AS $$
CLUSTER 'testcluster';
RUN ON 0;
SELECT * FROM remote_func(a, c, b);
$$ LANGUAGE plproxy;

select * from test3(NULL,NULL, 'a');
select * from test3('a', NULL,NULL);









