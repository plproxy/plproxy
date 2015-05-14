
-- handler function
CREATE FUNCTION plproxy_call_handler ()
RETURNS language_handler AS 'plproxy' LANGUAGE C;

-- validator function
CREATE FUNCTION plproxy_validator (oid)
RETURNS void AS 'plproxy' LANGUAGE C;

-- language
CREATE LANGUAGE plproxy HANDLER plproxy_call_handler VALIDATOR plproxy_validator;

