
-------------------------------------------------
-- encoding tests
-------------------------------------------------

-- google translate says:
-- column: コラム
-- table: テーブル
-- client data: クライアント側のデータ
-- proxy data: プロキシデータ
-- remote data: リモートデータ
-- argument: 引数


set client_min_messages = 'warning';
drop database if exists test_enc_proxy;
drop database if exists test_enc_part;
create database test_enc_proxy with encoding 'euc_jp';
create database test_enc_part with encoding 'utf-8';

-- initialize proxy db
\c test_enc_proxy
create language plpgsql;
\i plproxy.sql
create schema plproxy;
create or replace function plproxy.get_cluster_version(cluster_name text)
returns integer as $$ begin return 1; end; $$ language plpgsql; 
create or replace function plproxy.get_cluster_config(cluster_name text, out key text, out val text)
returns setof record as $$ begin return; end; $$ language plpgsql;
create or replace function plproxy.get_cluster_partitions(cluster_name text)
returns setof text as $$ begin
    return next 'host=127.0.0.1 dbname=test_enc_part'; return;
end; $$ language plpgsql;

-- initialize part db
\c test_enc_part
set client_encoding = 'utf8';
create table intl_data (id int4, "コラム" text);
insert into intl_data values (1, 'リモートデータ');
set client_encoding = 'sjis';
select * from intl_data order by 1;
set client_encoding = 'euc_jp';
select * from intl_data order by 1;
set client_encoding = 'utf-8';
select * from intl_data order by 1;

-- test
\c test_enc_proxy
set client_encoding = 'utf8';
create table intl_data (id int4, "コラム" text);
create function test_encoding() returns setof intl_data as $$
    cluster 'testcluster'; run on 0; select * from intl_data order by 1;
$$ language plproxy;
set client_encoding = 'sjis';
select * from test_encoding();
set client_encoding = 'euc_jp';
select * from test_encoding();
set client_encoding = 'utf8';
select * from test_encoding();

\c template1
set client_min_messages = 'warning';
drop database if exists test_enc_proxy;
drop database if exists test_enc_part;
create database test_enc_proxy with encoding 'utf-8';
create database test_enc_part with encoding 'euc_jp';

-- initialize proxy db
\c test_enc_proxy
create language plpgsql;
\i plproxy.sql
create schema plproxy;
create or replace function plproxy.get_cluster_version(cluster_name text)
returns integer as $$ begin return 1; end; $$ language plpgsql; 
create or replace function plproxy.get_cluster_config(cluster_name text, out key text, out val text)
returns setof record as $$ begin return; end; $$ language plpgsql;
create or replace function plproxy.get_cluster_partitions(cluster_name text)
returns setof text as $$ begin
    return next 'host=127.0.0.1 dbname=test_enc_part'; return;
end; $$ language plpgsql;

-- initialize part db
\c test_enc_part
set client_encoding = 'utf8';
create table intl_data (id int4, "コラム" text);
insert into intl_data values (1, 'リモートデータ');
set client_encoding = 'sjis';
select * from intl_data order by 1;
set client_encoding = 'euc_jp';
select * from intl_data order by 1;
set client_encoding = 'utf-8';
select * from intl_data order by 1;

-- test
\c test_enc_proxy
set client_encoding = 'utf8';
create table intl_data (id int4, "コラム" text);
create function test_encoding() returns setof intl_data as $$
    cluster 'testcluster'; run on 0; select * from intl_data order by 1;
$$ language plproxy;
set client_encoding = 'sjis';
select * from test_encoding();
set client_encoding = 'euc_jp';
select * from test_encoding();
set client_encoding = 'utf-8';
select * from test_encoding();


