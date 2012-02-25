
\set ECHO none

set client_min_messages = 'warning';

\i sql/plproxy.sql

set client_min_messages = 'fatal';
create language plpgsql;
set client_min_messages = 'warning';

-- create cluster info functions
create schema plproxy;
create or replace function plproxy.get_cluster_version(cluster_name text)
returns integer as $$
begin
    if cluster_name = 'testcluster' then
        return 5;
    end if;
    if cluster_name = 'badcluster' then
        return 5;
    end if;
    raise exception 'no such cluster: %', cluster_name;
end; $$ language plpgsql;

create or replace function
plproxy.get_cluster_partitions(cluster_name text)
returns setof text as $$
begin
    if cluster_name = 'testcluster' then
        return next 'host=127.0.0.1 dbname=test_part';
        return;
    end if;
    if cluster_name = 'badcluster' then
        return next 'host=127.0.0.1 dbname=nonex_db';
        return;
    end if;
    raise exception 'no such cluster: %', cluster_name;
end; $$ language plpgsql;

create or replace function
plproxy.get_cluster_config(cluster_name text, out key text, out val text)
returns setof record as $$
begin
    key = 'keepalive_idle';     val = '240'; return next;
    key = 'keepalive_interval'; val = '15'; return next;
    key = 'keepalive_count';    val = '4'; return next;
    return;
end; $$ language plpgsql;

-------------------------------------------------
-- intialize part
-------------------------------------------------
drop database if exists test_part;
drop database if exists test_part0;
drop database if exists test_part1;
drop database if exists test_part2;
drop database if exists test_part3;
create database test_part;
create database test_part0;
create database test_part1;
create database test_part2;
create database test_part3;

drop database if exists test_enc_proxy;
drop database if exists test_enc_part;

\c test_part
set client_min_messages = 'fatal';
create language plpgsql;
\c test_part0
set client_min_messages = 'fatal';
create language plpgsql;
\c test_part1
set client_min_messages = 'fatal';
create language plpgsql;
\c test_part2
set client_min_messages = 'fatal';
create language plpgsql;
\c test_part3
set client_min_messages = 'fatal';
create language plpgsql;
