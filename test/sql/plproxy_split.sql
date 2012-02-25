-- partition functions
\c test_part0
create or replace function test_array(a text[], b text[], c text) returns text as
$$
select current_database() || ' $1:' || array_to_string($1, ',')
                          || ' $2:' || array_to_string($2, ',')
                          || ' $3:' || $3;
$$ language sql;
\c test_part1
create or replace function test_array(a text[], b text[], c text) returns text as
$$
select current_database() || ' $1:' || array_to_string($1, ',')
                          || ' $2:' || array_to_string($2, ',')
                          || ' $3:' || $3;
$$ language sql;
\c test_part2
create or replace function test_array(a text[], b text[], c text) returns text as
$$
select current_database() || ' $1:' || array_to_string($1, ',')
                          || ' $2:' || array_to_string($2, ',')
                          || ' $3:' || $3;
$$ language sql;
\c test_part3
create or replace function test_array(a text[], b text[], c text) returns text as
$$
select current_database() || ' $1:' || array_to_string($1, ',')
                          || ' $2:' || array_to_string($2, ',')
                          || ' $3:' || $3;
$$ language sql;

\c regression

-- invalid arg reference
create or replace function test_array(a text[], b text[], c text) returns setof text as
$$ split $4; cluster 'testcluster'; run on 0;$$ language plproxy;
select * from test_array(array['a'], array['g'], 'foo');

-- invalid arg name
create or replace function test_array(a text[], b text[], c text) returns setof text as
$$ split x; cluster 'testcluster'; run on 0; $$ language plproxy;
select * from test_array(array['a'], array['b', 'c'], 'foo');

-- cannot split more than once
create or replace function test_array(a text[], b text[], c text) returns setof text as
$$ split a, b, b; cluster 'testcluster'; run on 0; $$ language plproxy;
select * from test_array(array['a'], array['b', 'c'], 'foo');

-- attempt to split non-array
create or replace function test_array(a text[], b text[], c text) returns setof text as
$$ split $3; cluster 'testcluster'; run on 0;$$ language plproxy;
select * from test_array(array['a'], array['g'], 'foo');

-- array size/dimensions mismatch
create or replace function test_array(a text[], b text[], c text) returns setof text as
$$ split a, b; cluster 'testcluster'; run on 0; $$ language plproxy;
select * from test_array(array['a'], array['b', 'c'], 'foo');
select * from test_array(array['a','b','c','d'], null, 'foo');
select * from test_array(null, array['e','f','g','h'], 'foo');
select * from test_array(array[array['a1'],array['a2']], array[array['b1'],array['b2']], 'foo');

-- run on array hash, split one array
create or replace function test_array(a text[], b text[], c text) returns setof text as
$$ split a; cluster 'testcluster'; run on ascii(a);$$ language plproxy;
select * from test_array(array['a','b','c','d'], array['e','f','g','h'], 'foo');

-- run on text hash, split two arrays (nop split)
create or replace function test_array(a text[], b text[], c text) returns setof text as
$$ split a, b; cluster 'testcluster'; run on ascii(c);$$ language plproxy;
select * from test_array(array['a','b','c','d'], array['e','f','g','h'], 'foo');

-- run on array hash, split two arrays
create or replace function test_array(a text[], b text[], c text) returns setof text as
$$ split a, b; cluster 'testcluster'; run on ascii(a);$$ language plproxy;
select * from test_array(array['a','b','c','d'], array['e','f','g','h'], 'foo');
select * from test_array(null, null, null);
select * from test_array('{}'::text[], '{}'::text[], 'foo');

-- run on text hash, split all arrays
create or replace function test_array(a text[], b text[], c text) returns setof text as
$$ split all; cluster 'testcluster'; run on ascii(c);$$ language plproxy;
select * from test_array(array['a','b','c','d'], array['e','f','g','h'], 'foo');

-- run on text hash, attempt to split all arrays but none are present
create or replace function test_nonarray_split(a text, b text, c text) returns setof text as
$$ split all; cluster 'testcluster'; run on ascii(a); select * from test_array(array[a], array[b], c);
$$ language plproxy;
select * from test_nonarray_split('a', 'b', 'c');

-- run on array hash, split all arrays
create or replace function test_array(a text[], b text[], c text) returns setof text as
$$ split all; cluster 'testcluster'; run on ascii(a);$$ language plproxy;
select * from test_array(array['a','b','c','d'], array['e','f','g','h'], 'foo');

-- run on arg
create or replace function test_array_direct(a integer[], b text[], c text) returns setof text as
$$ split a; cluster 'testcluster'; run on a; select test_array('{}'::text[], b, c);$$ language plproxy;

select * from test_array_direct(array[2,3], array['a','b','c','d'], 'foo');

create or replace function test_array_direct(a integer[], b text[], c text) returns setof text as
$$ split a, b; cluster 'testcluster'; run on a; select test_array('{}'::text[], b, c);$$ language plproxy;

select * from test_array_direct(array[0,1,2,3], array['a','b','c','d'], 'foo');
