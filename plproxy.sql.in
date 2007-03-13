
-- handler function
CREATE FUNCTION plproxy_call_handler ()
RETURNS language_handler AS 'MODULE_PATHNAME' LANGUAGE C;

-- language
CREATE LANGUAGE plproxy HANDLER plproxy_call_handler;

