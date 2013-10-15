\set VERBOSITY terse

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
select 1 from (select set_config(name, 'escape', false) as ignore
          from pg_settings where name = 'bytea_output') x
where x.ignore = 'foo';
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
as $$ connect 'host=127.0.0.1 dbname=test_part'; select current_database(); $$ language plproxy;
select * from test_connect1();

-- test CONNECT $argument
create function test_connect2(connstr text) returns text
as $$ connect connstr; select current_database(); $$ language plproxy;
select * from test_connect2('host=127.0.0.1 dbname=test_part');

-- test CONNECT function($argument)
create function test_connect3(connstr text) returns text
as $$ connect text(connstr); select current_database(); $$ language plproxy;
select * from test_connect3('host=127.0.0.1 dbname=test_part');

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

-- test arg type quoting
create domain "bad type" as text;
create function test_argq(username text, "some arg" integer, "other arg" "bad type",
                          out "bad out" text, out "bad out2" "bad type")
as $$ cluster 'testcluster'; run on hashtext(username); $$ language plproxy;
\c test_part
create domain "bad type" as text;
create function test_argq(username text, "some arg" integer, "other arg" "bad type",
                          out "bad out" text, out "bad out2" "bad type")
                    as $$ begin return; end; $$ language plpgsql;
\c regression
select * from test_argq('user', 1, 'q');

-- test hash types function
create or replace function t_hash16(int4) returns int2 as $$
declare
    res int2;
begin
    res = $1::int2;
    return res;
end;
$$ language plpgsql;

create or replace function t_hash64(int4) returns int8 as $$
declare
    res int8;
begin
    res = $1;
    return res;
end;
$$ language plpgsql;

create function test_hash16(id integer, data text)
returns text as $$ cluster 'testcluster'; run on t_hash16(id); select data; $$ language plproxy;
select * from test_hash16('0', 'hash16');

create function test_hash64(id integer, data text)
returns text as $$ cluster 'testcluster'; run on t_hash64(id); select data; $$ language plproxy;
select * from test_hash64('0', 'hash64');

-- test argument difference
\c test_part
create function test_difftypes(username text, out val1 int2, out val2 float8)
as $$ begin val1 = 1; val2 = 3;return; end; $$ language plpgsql;
\c regression
create function test_difftypes(username text, out val1 int4, out val2 float4)
as $$ cluster 'testcluster'; run on 0; $$ language plproxy;
select * from test_difftypes('types');

-- test simple hash
\c test_part
create function test_simple(partno int4) returns int4
as $$ begin return $1; end; $$ language plpgsql;
\c regression
create function test_simple(partno int4) returns int4
as $$
    cluster 'testcluster';
    run on $1;
$$ language plproxy;
select * from test_simple(0);
drop function test_simple(int4);

create function test_simple(partno int4) returns int4
as $$
    cluster 'testcluster';
    run on partno;
$$ language plproxy;
select * from test_simple(0);

-- test error passing
\c test_part
create function test_error1() returns int4
as $$
begin
    select line2err;
    return 0;
end;
$$ language plpgsql;
\c regression
create function test_error1() returns int4
as $$
    cluster 'testcluster';
    run on 0;
$$ language plproxy;
select * from test_error1();

create function test_error2() returns int4
as $$
    cluster 'testcluster';
    run on 0;
    select err;
$$ language plproxy;
select * from test_error2();

create function test_error3() returns int4
as $$
    connect 'host=127.0.0.1 dbname=test_part';
$$ language plproxy;
select * from test_error3();

-- test invalid db
create function test_bad_db() returns int4
as $$
    cluster 'badcluster';
$$ language plproxy;
select * from test_bad_db();

create function test_bad_db2() returns int4
as $$
    connect 'host=127.0.0.1 dbname=wrong_name_db';
$$ language plproxy;
select * from test_bad_db2();


