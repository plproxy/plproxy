
set client_min_messages = 'warning';

drop foreign data wrapper if exists plproxy cascade;
create foreign data wrapper plproxy;

create server sqlmedcluster foreign data wrapper plproxy 
    options (   partition_0 'dbname=test_part3 host=localhost',
                partition_1 'dbname=test_part2 host=localhost',
                partition_2 'dbname=test_part1 host=localhost',
                partition_3 'dbname=test_part0 host=localhost');

create or replace function sqlmed_test1() returns setof text as $$
    cluster 'sqlmedcluster';
    run on 0;
    select 'plproxy: user=' || current_user || ' dbname=' || current_database();
$$ language plproxy;

drop user if exists test_user_bob;
create user test_user_bob password 'secret';

-- no user mapping
set session authorization test_user_bob;
select * from sqlmed_test1();
reset session authorization;

-- add a public user mapping 
create user mapping for public server sqlmedcluster
    options (   user        'test_user_bob',
                password    'secret1');

-- no access to foreign server
set session authorization test_user_bob;
select * from sqlmed_test1();
reset session authorization;

-- ok, access granted
grant usage on foreign server sqlmedcluster to test_user_bob;
set session authorization test_user_bob;
select * from sqlmed_test1();
reset session authorization;

-- cluster definition validation

-- partition numbers must be consecutive
alter server sqlmedcluster options (drop partition_2);
select * from sqlmed_test1();

-- invalid partition count
alter server sqlmedcluster options 
    (drop partition_3,
     add  partition_2 'dbname=test_part1 host=localhost');
select * from sqlmed_test1();

-- switching betweem SQL/MED and compat mode

create or replace function sqlmed_compat_test() returns setof text as $$
    cluster 'testcluster';
    run on 0;
    select 'plproxy: part=' || current_database();
$$ language plproxy;

-- testcluster
select * from sqlmed_compat_test();

-- override the test cluster with a SQL/MED definition
drop server if exists testcluster cascade;
create server testcluster foreign data wrapper plproxy 
    options (partition_0 'dbname=regression host=localhost');
create user mapping for public server testcluster;

-- sqlmed testcluster
select * from sqlmed_compat_test();

-- now drop the SQL/MED testcluster, and test fallback
drop server testcluster cascade;

-- back on testcluster again
select * from sqlmed_compat_test();

