\set VERBOSITY terse

-- test bad arg
create function test_err1(dat text)
returns text as $$
    cluster 'testcluster';
    run on hashtext(username);
$$ language plproxy;
select * from test_err1('dat');

create function test_err2(dat text)
returns text as $$
    cLuStEr 'testcluster';
    rUn oN hAshtext($2);
$$ language plproxy;
select * from test_err2('dat');

create function test_err3(dat text)
returns text as $$
    cluster 'nonexists';
    run on hashtext($1);
$$ language plproxy;
select * from test_err3('dat');

-- should work
create function test_err_none(dat text)
returns text as $$
    cluster 'testcluster';
    run on hashtext($1);
    select 'ok';
$$ language plproxy;
select * from test_err_none('dat');

--- result map errors
create function test_map_err1(dat text)
returns text as $$ cluster 'testcluster'; run on 0;
    select dat as "foo", 'asd' as "bar";
$$ language plproxy;
select * from test_map_err1('dat');

create function test_map_err2(dat text, out res1 text, out res2 text)
returns record as $$ cluster 'testcluster'; run on 0;
    select dat as res1;
$$ language plproxy;
select * from test_map_err2('dat');

create function test_map_err3(dat text, out res1 text, out res2 text)
returns record as $$ cluster 'testcluster'; run on 0;
    select dat as res1, 'foo' as res_none;
$$ language plproxy;
select * from test_map_err3('dat');

create function test_map_err4(dat text, out res1 text, out res2 text)
returns record as $$
    --cluster 'testcluster';
    run on hashtext(dat);
    select dat as res2, 'foo' as res1;
$$ language plproxy;
select * from test_map_err4('dat');

create function test_variadic_err(first text, rest variadic text[])
returns text as $$
    cluster 'testcluster';
$$ language plproxy;
select * from test_variadic_err('dat', 'dat', 'dat');

create function test_volatile_err(dat text)
returns text
stable
as $$
    cluster 'testcluster';
$$ language plproxy;
select * from test_volatile_err('dat');

create function test_pseudo_arg_err(dat cstring)
returns text
as $$
    cluster 'testcluster';
$$ language plproxy;
select * from test_pseudo_arg_err(textout('dat'));

create function test_pseudo_ret_err(dat text)
returns cstring
as $$
    cluster 'testcluster';
$$ language plproxy;
-- not detected in validator
select * from test_pseudo_ret_err('dat');

create function test_runonall_err(dat text)
returns text
as $$
    cluster 'testcluster';
    run on all;
$$ language plproxy;
select * from test_runonall_err('dat');

-- make sure that errors from non-setof functions returning <> 1 row have
-- a proper sqlstate
create function test_no_results_plproxy()
returns int
as $$
    cluster 'testcluster';
    run on any;
    select 1 from pg_database where datname = '';
$$ language plproxy;
create function test_no_results()
returns void
as $$
begin
    begin
        perform test_no_results_plproxy();
    exception when no_data_found then
        null;
    end;
end;
$$ language plpgsql;
select * from test_no_results();

create function test_multi_results_plproxy()
returns int
as $$
    cluster 'testcluster';
    run on any;
    select 1 from pg_database;
$$ language plproxy;
create function test_multi_results()
returns void
as $$
begin
    begin
        perform test_multi_results_plproxy();
    exception when too_many_rows then
        null;
    end;
end;
$$ language plpgsql;
select * from test_multi_results();
