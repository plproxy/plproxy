ALTER EXTENSION plproxy ADD FUNCTION plproxy_call_handler();
ALTER EXTENSION plproxy ADD LANGUAGE plproxy;
ALTER EXTENSION plproxy ADD FUNCTION plproxy_fdw_validator(text[], oid);
ALTER EXTENSION plproxy ADD FOREIGN DATA WRAPPER plproxy;

