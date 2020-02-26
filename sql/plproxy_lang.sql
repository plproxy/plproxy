
-- handler function
CREATE OR REPLACE FUNCTION plproxy_call_handler ()
RETURNS language_handler AS 'plproxy' LANGUAGE C;

-- validator function
CREATE OR REPLACE FUNCTION plproxy_validator (oid)
RETURNS void AS 'plproxy' LANGUAGE C;

-- language
CREATE OR REPLACE LANGUAGE plproxy HANDLER plproxy_call_handler VALIDATOR plproxy_validator;

