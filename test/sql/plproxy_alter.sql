
\c test_part0

create function test_table1(out id int4, out data text, out data2 text, out data3 text)
as $$
begin
    id = 1;
    data = 'Data1';
    data2 = 'Data2';
    data3 = 'Data3';
    return;
end;
$$ language plpgsql;

select * from test_table1();

\c regression
create table ret_table (
    id int4,
    data text
);

create function test_table1()
returns ret_table as $$
    cluster 'testcluster';
    run on 0;
$$ language plproxy;

select * from test_table1();


-- add column
alter table ret_table add column data2 text;
select * from test_table1();
\c regression
select * from test_table1();

-- drop & add
alter table ret_table drop column data2;
alter table ret_table add column data3 text;
select * from test_table1();
\c regression
select * from test_table1();

-- drop
alter table ret_table drop column data3;
select * from test_table1();
\c regression
select * from test_table1();

-- add again
alter table ret_table add column data3 text;
alter table ret_table add column data2 text;
select * from test_table1();
\c regression
select * from test_table1();

