
set client_min_messages = 'warning';

drop database if exists test_enc_proxy;
drop database if exists test_enc_part;

create database test_enc_proxy with encoding 'utf-8';
create database test_enc_part with encoding 'euc_jp';

\c test_enc_proxy
create language plpgsql;

\i plproxy.sql

-- create cluster info functions
create schema plproxy;

create or replace function plproxy.get_cluster_version(cluster_name text)
returns integer as $$ begin return 1; end; $$ language plpgsql; 

create or replace function
plproxy.get_cluster_partitions(cluster_name text)
returns setof text as $$
begin
    if cluster_name = 'testcluster' then
        return next 'host=127.0.0.1 dbname=test_enc_part';
        return;
    end if;
    raise exception 'no such cluster: %', cluster_name;
end; $$ language plpgsql;

create or replace function plproxy.get_cluster_config(cluster_name text, out key text, out val text)
returns setof record as $$ begin return; end; $$ language plpgsql;

-------------------------------------------------
-- intialize part
-------------------------------------------------

\c test_enc_part
set client_encoding = 'utf8';

create table intl_data (
    id serial,
    val text
);
-- insert into intl_data (val) values ('õäöüÕÄÖÜ');
insert into intl_data (val) values ('日本につきましては、');

select id, val from intl_data order by 1;

set client_encoding = 'sjis';
select id, val from intl_data order by 1;
set client_encoding = 'euc_jp';
select id, val from intl_data order by 1;

\c test_enc_proxy

create function test_encoding(out id int4, out val text)
returns setof record as $$
    cluster 'testcluster';
    run on 0;
    select id, val from intl_data order by 1;
$$ language plproxy;

set client_encoding = 'utf8';
select * from test_encoding();
set client_encoding = 'sjis';
select * from test_encoding();
set client_encoding = 'euc_jp';
select * from test_encoding();

