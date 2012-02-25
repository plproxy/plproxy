
-- test bad arg
create function test_err1(dat text)
returns text as $$
    cluster 'testcluster';
    run on hashtext(username);
$$ language plproxy;
select * from test_err1('dat');

create function test_err2(dat text)
returns text as $$
    cluster 'testcluster';
    run on hashtext($2);
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





