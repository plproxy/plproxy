
-- test target clause

create function test_target(xuser text, tmp boolean)
returns text as $$
    cluster 'testcluster';
    run on 0;
    target test_target_dst;
$$ language plproxy;

\c test_part0

create function test_target_dst(xuser text, tmp boolean)
returns text as $$
begin
    return 'dst';
end;
$$ language plpgsql;

\c regression

select * from test_target('foo', true);

-- test errors

create function test_target_err1(xuser text)
returns text as $$
    cluster 'testcluster';
    run on 0;
    target test_target_dst;
    target test_target_dst;
$$ language plproxy;

select * from test_target_err1('asd');

create function test_target_err2(xuser text)
returns text as $$
    cluster 'testcluster';
    run on 0;
    target test_target_dst;
    select 1;
$$ language plproxy;

select * from test_target_err2('asd');

