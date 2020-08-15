
\set VERBOSITY terse
set client_min_messages = 'warning';

create server modcluster1 foreign data wrapper plproxy
    options (
        partition_0 'dbname=test_part0 host=localhost',
        partition_1 'dbname=test_part1 host=localhost',
        partition_2 'dbname=test_part2 host=localhost',
        modular_mapping '1'
    );

drop user if exists test_user_mod;
create user test_user_mod;
create user mapping for public server modcluster1 options (user 'test_user_mod');
grant usage on foreign server modcluster1 to test_user_mod;

create or replace function mod1_test(hash int) returns setof text as $$
    cluster 'modcluster1';
    run on hash;
    select 'plproxy: user=' || current_user || ' dbname=' || current_database();
$$ language plproxy;

set session authorization test_user_mod;

select mod1_test(0);
select mod1_test(1);
select mod1_test(2);
select mod1_test(3);
select mod1_test(4);
select mod1_test(-1);
select mod1_test(-2);
select mod1_test(-3);
select mod1_test(-4);

reset session authorization;

