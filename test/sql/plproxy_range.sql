
\c test_part0

CREATE TYPE f8range AS RANGE (
        subtype = float8,
        subtype_diff = float8mi
);

create or replace function test_range(in id int, inout frange f8range,
    out irange int4range)
returns record as $$
begin
    irange := '[20,30)';
    return;
end;
$$ language plpgsql;

select * from test_range(0, '(1.5,2.4]');

\c regression

CREATE TYPE f8range AS RANGE (
        subtype = float8,
        subtype_diff = float8mi
);

create or replace function test_range(in _id integer, inout frange f8range, out irange int4range)
returns setof record as $$
    cluster 'testcluster';
    run on _id;
$$ language plproxy;

select * from test_range(0, '(1.5,2.4]');

