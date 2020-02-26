CREATE OR REPLACE FUNCTION plproxy_validator (oid)
RETURNS void AS 'plproxy' LANGUAGE C;

CREATE OR REPLACE LANGUAGE plproxy HANDLER plproxy_call_handler VALIDATOR plproxy_validator;
