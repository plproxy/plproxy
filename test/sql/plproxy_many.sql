create or replace function plproxy.get_cluster_version(cluster_name text)
returns integer as $$
begin
    if cluster_name = 'testcluster' then
        return 6;
    end if;
    raise exception 'no such cluster: %', cluster_name;
end; $$ language plpgsql;

create or replace function plproxy.get_cluster_partitions(cluster_name text)
returns setof text as $$
begin
    if cluster_name = 'testcluster' then
        return next 'host=127.0.0.1 dbname=test_part0';
        return next 'host=127.0.0.1 dbname=test_part1';
        return next 'host=127.0.0.1 dbname=test_part2';
        return next 'host=127.0.0.1 dbname=test_part3';
        return;
    end if;
    raise exception 'no such cluster: %', cluster_name;
end; $$ language plpgsql;

\c test_part0
create function test_multi(part integer, username text)
returns integer as $$ begin return 0; end; $$ language plpgsql;
\c test_part1
create function test_multi(part integer, username text)
returns integer as $$ begin return 1; end; $$ language plpgsql;
\c test_part2
create function test_multi(part integer, username text)
returns integer as $$ begin return 2; end; $$ language plpgsql;
\c test_part3
create function test_multi(part integer, username text)
returns integer as $$ begin return 3; end; $$ language plpgsql;

\c regression
create function test_multi(part integer, username text)
returns integer as $$ cluster 'testcluster'; run on int4(part); $$ language plproxy;
select test_multi(0, 'foo');
select test_multi(1, 'foo');
select test_multi(2, 'foo');
select test_multi(3, 'foo');

-- test RUN ON ALL
drop function test_multi(integer, text);
create function test_multi(part integer, username text)
returns setof integer as $$ cluster 'testcluster'; run on all; $$ language plproxy;
select test_multi(0, 'foo') order by 1;

-- test RUN ON 2
drop function test_multi(integer, text);
create function test_multi(part integer, username text)
returns setof integer as $$ cluster 'testcluster'; run on 2; $$ language plproxy;
select test_multi(0, 'foo');

-- test RUN ON RANDOM
drop function test_multi(integer, text);
create function test_multi(part integer, username text)
returns setof integer as $$ cluster 'testcluster'; run on any; $$ language plproxy;
-- expect that 20 calls use all partitions
select distinct test_multi(0, 'foo') from generate_series(1,20) order by 1;


