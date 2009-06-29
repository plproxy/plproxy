
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
