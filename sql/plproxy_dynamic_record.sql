-- dynamic query support testing 
create or replace function dynamic_query(q text)
returns setof record as $x$
    cluster 'map0';
    run on all;
$x$ language plproxy;

\c test_part0
create or replace function dynamic_query(q text)
returns setof record as $x$
declare                            
    ret record;                      
begin                              
    for ret in execute q loop   
        return next ret;               
    end loop;                        
    return;                          
end;    
$x$ language plpgsql;
create table dynamic_query_test (
    id integer,
    username text, 
		other	text
);
insert into dynamic_query_test values ( 1, 'user1', 'blah');
insert into dynamic_query_test values ( 2, 'user2', 'foo');

\c regression
select * from dynamic_query('select * from dynamic_query_test') as (id integer, username text, other text);
select * from dynamic_query('select id, username from dynamic_query_test') as foo(id integer, username text);


-- test errors
create or replace function dynamic_query_select()
returns setof record as $x$
    cluster 'map0';
    run on all;
    select id, username from dynamic_query_test;
$x$ language plproxy;
select * from dynamic_query_select() as (id integer, username text);

