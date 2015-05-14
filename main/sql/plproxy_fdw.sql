-- validator function
CREATE FUNCTION plproxy_fdw_validator (text[], oid)
RETURNS boolean AS 'plproxy' LANGUAGE C;

-- foreign data wrapper
CREATE FOREIGN DATA WRAPPER plproxy VALIDATOR plproxy_fdw_validator;

