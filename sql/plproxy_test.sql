
-- test normal function
create function testfunc(username text, id integer, data text)
returns text as $$ cluster 'testcluster'; run on hashtext(username); $$ language plproxy;
\c test_part
create function testfunc(username text, id integer, data text)
returns text as $$ begin return 'username=' || username; end; $$ language plpgsql;
\c regression
select * from testfunc('user', 1, 'foo');
select * from testfunc('user', 1, 'foo');
select * from testfunc('user', 1, 'foo');


-- test setof text
create function test_set(username text, num integer)
returns setof text as $$ cluster 'testcluster'; run on hashtext(username); $$ language plproxy;
\c test_part
create function test_set(username text, num integer)
returns setof text as $$
declare i integer;
begin
    i := 0;
    while i < num loop
        return next 'username=' || username || ' row=' || i;
        i := i + 1;
    end loop;
    return;
end; $$ language plpgsql;
\c regression
select * from test_set('user', 1);
select * from test_set('user', 0);
select * from test_set('user', 3);

-- test record
create type ret_test_rec as ( id integer, dat text);
create function test_record(username text, num integer)
returns ret_test_rec as $$ cluster 'testcluster'; run on hashtext(username); $$ language plproxy;
\c test_part
create type ret_test_rec as ( id integer, dat text);
create function test_record(username text, num integer)
returns ret_test_rec as $$
declare ret ret_test_rec%rowtype;
begin
    ret := (num, username);
    return ret;
end; $$ language plpgsql;
\c regression
select * from test_record('user', 3);

-- test setof record
create function test_record_set(username text, num integer)
returns setof ret_test_rec as $$ cluster 'testcluster'; run on hashtext(username); $$ language plproxy;
\c test_part
create function test_record_set(username text, num integer)
returns setof ret_test_rec as $$
declare ret ret_test_rec%rowtype; i integer;
begin
    i := 0;
    while i < num loop
        ret := (i, username);
        i := i + 1;
        return next ret;
    end loop;
    return;
end; $$ language plpgsql;
\c regression
select * from test_record_set('user', 1);
select * from test_record_set('user', 0);
select * from test_record_set('user', 3);


-- test void
create function test_void(username text, num integer)
returns void as $$ cluster 'testcluster'; run on hashtext(username); $$ language plproxy;
\c test_part
create function test_void(username text, num integer)
returns void as $$
begin
    return;
end; $$ language plpgsql;
-- look what void actually looks
select * from test_void('void', 2);
select test_void('void', 2);
\c regression
select * from test_void('user', 1);
select * from test_void('user', 3);
select test_void('user', 3);
select test_void('user', 3);


-- test normal outargs
create function test_out1(username text, id integer, out data text)
as $$ cluster 'testcluster'; run on hashtext(username); $$ language plproxy;
\c test_part
create function test_out1(username text, id integer, out data text)
returns text as $$ begin data := 'username=' || username; return; end; $$ language plpgsql;
\c regression
select * from test_out1('user', 1);

-- test complicated outargs
create function test_out2(username text, id integer, out out_id integer, xdata text, inout xdata2 text, out odata text)
as $$ cluster 'testcluster'; run on hashtext(username); $$ language plproxy;
\c test_part
create function test_out2(username text, id integer, out out_id integer, xdata text, inout xdata2 text, out odata text)
as $$ begin
    out_id = id;
    xdata2 := xdata2 || xdata;
    odata := 'username=' || username;
    return;
end; $$ language plpgsql;
\c regression
select * from test_out2('user', 1, 'xdata', 'xdata2');

-- test various types
create function test_types(username text, inout vbool boolean, inout xdate timestamp, inout bin bytea)
as $$ cluster 'testcluster'; run on hashtext(username); $$ language plproxy;
\c test_part
create function test_types(username text, inout vbool boolean, inout xdate timestamp, inout bin bytea)
as $$ begin return; end; $$ language plpgsql;
\c regression
select * from test_types('types', true, '2009-11-04 12:12:02', E'a\\000\\001\\002b');
select * from test_types('types', NULL, NULL, NULL);


-- test user defined types
create domain posint as int4 check (value > 0);
create type struct as (id int4, data text);

create function test_types2(username text, inout v_posint posint, inout v_struct struct, inout arr int8[])
as $$ cluster 'testcluster'; $$ language plproxy;

\c test_part
create domain posint as int4 check (value > 0);
create type struct as (id int4, data text);
create function test_types2(username text, inout v_posint posint, inout v_struct struct, inout arr int8[])
as $$ begin return; end; $$ language plpgsql;
\c regression
select * from test_types2('types', 4, (2, 'asd'), array[1,2,3]);
select * from test_types2('types', NULL, NULL, NULL);

-- test CONNECT
create function test_connect1() returns text
as $$ connect 'dbname=test_part'; select current_database(); $$ language plproxy;
select * from test_connect1();


-- test quoting function
create type "RetWeird" as (
    "ColId" int4,
    "ColData" text
);

create function "testQuoting"(username text, id integer, data text)
returns "RetWeird" as $$ cluster 'testcluster'; run on hashtext(username); $$ language plproxy;
\c test_part
create type "RetWeird" as (
    "ColId" int4,
    "ColData" text
);
create function "testQuoting"(username text, id integer, data text)
returns "RetWeird" as $$ select 1::int4, 'BazOoka'::text $$ language sql;
\c regression
select * from "testQuoting"('user', '1', 'dat');

