/*
 * PL/Proxy - easy access to partitioned database.
 *
 * Copyright (c) 2006 Sven Suursoho, Skype Technologies OÜ
 * Copyright (c) 2007 Marko Kreen, Skype Technologies OÜ
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

/*
 * Cluster info management.
 *
 * Info structures are kept in separate memory context: cluster_mem.
 */

#include "plproxy.h"

/* Permanent memory area for cluster info structures */
static MemoryContext cluster_mem;

/*
 * Singly linked list of clusters.
 *
 * For searching by name.  If there will be lots of clusters
 * should use some faster search method, HTAB probably.
 */
static ProxyCluster *cluster_list = NULL;

/*
 * Similar list for fake clusters (for CONNECT functions).
 *
 * Cluster name will be actual connect string.
 */
static ProxyCluster *fake_cluster_list = NULL;

/* plan for fetching cluster version */
static void *version_plan;

/* plan for fetching cluster partitions */
static void *partlist_plan;

/* plan for fetching cluster config */
static void *config_plan;

/* query for fetching cluster version */
static const char version_sql[] = "select * from plproxy.get_cluster_version($1)";

/* query for fetching cluster partitions */
static const char part_sql[] = "select * from plproxy.get_cluster_partitions($1)";

/* query for fetching cluster config */
static const char config_sql[] = "select * from plproxy.get_cluster_config($1)";

#ifdef PLPROXY_USE_SQLMED

/* list of all the valid configuration options to plproxy cluster */
static const char *cluster_config_options[] = {
	"statement_timeout",
	"connection_lifetime",
	"query_timeout",
	"disable_binary",
	"keepalive_idle",
	"keepalive_interval",
	"keepalive_count",
	NULL
};

extern Datum plproxy_fdw_validator(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(plproxy_fdw_validator);

#endif

/*
 * Connsetion count should be non-zero and power of 2.
 */
static bool
check_valid_partcount(int n)
{
	return (n > 0) && !(n & (n - 1));
}

/*
 * Create cache memory area and prepare plans
 */
void
plproxy_cluster_cache_init(void)
{
	/*
	 * create long-lived memory context
	 */

	cluster_mem = AllocSetContextCreate(TopMemoryContext,
										"PL/Proxy cluster context",
										ALLOCSET_SMALL_MINSIZE,
										ALLOCSET_SMALL_INITSIZE,
										ALLOCSET_SMALL_MAXSIZE);
}

/* initialize plans on demand */
static void
plproxy_cluster_plan_init(void)
{
	void	   *tmp_ver_plan, *tmp_part_plan, *tmp_conf_plan;
	Oid			types[] = {TEXTOID};
	static int	init_done = 0;

	if (init_done)
		return;

	/*
	 * prepare plans for fetching configuration.
	 */

	tmp_ver_plan = SPI_prepare(version_sql, 1, types);
	if (tmp_ver_plan == NULL)
		elog(ERROR, "PL/Proxy: plproxy.get_cluster_version() SQL fails: %s",
			 SPI_result_code_string(SPI_result));

	tmp_part_plan = SPI_prepare(part_sql, 1, types);
	if (tmp_part_plan == NULL)
		elog(ERROR, "PL/Proxy: plproxy.get_cluster_partitions() SQL fails: %s",
			 SPI_result_code_string(SPI_result));

	tmp_conf_plan = SPI_prepare(config_sql, 1, types);
	if (tmp_conf_plan == NULL)
		elog(ERROR, "PL/Proxy: plproxy.get_cluster_config() SQL fails: %s",
			 SPI_result_code_string(SPI_result));

	/*
	 * Store them only if all successful.
	 */
	version_plan = SPI_saveplan(tmp_ver_plan);
	partlist_plan = SPI_saveplan(tmp_part_plan);
	config_plan = SPI_saveplan(tmp_conf_plan);

	init_done = 1;
}

/*
 * Drop partition and connection data from cluster.
 */
static void
free_connlist(ProxyCluster *cluster)
{
	int			i;
	ProxyConnection *conn;

	for (i = 0; i < cluster->conn_count; i++)
	{
		conn = &cluster->conn_list[i];
		if (conn->db)
			PQfinish(conn->db);
		if (conn->res)
			PQclear(conn->res);
		if (conn->connstr)
			pfree((void *) conn->connstr);
	}
	pfree(cluster->part_map);
	pfree(cluster->conn_list);

	cluster->part_map = NULL;
	cluster->part_count = 0;
	cluster->part_mask = 0;
	cluster->conn_list = NULL;
	cluster->conn_count = 0;
}

/*
 * Add new database connection if it does not exists.
 */
static ProxyConnection *
add_connection(ProxyCluster *cluster, char *connstr, int part_num)
{
	int			i;
	ProxyConnection *conn = NULL;
	char	   *username;
	StringInfo	final;

	final = makeStringInfo();
	appendStringInfoString(final, connstr);

	/* append current user if not specified in connstr */
	if (strstr(connstr, "user=") == NULL)
	{
		username = GetUserNameFromId(GetSessionUserId());
		appendStringInfo(final, " user=%s", username);
	}

	/* check if already have it */
	for (i = 0; i < cluster->conn_count && !conn; i++)
	{
		ProxyConnection *c = &cluster->conn_list[i];

		if (strcmp(c->connstr, final->data) == 0)
			conn = c;
	}

	/* add new connection */
	if (!conn)
	{
		conn = &cluster->conn_list[cluster->conn_count++];
		conn->connstr = MemoryContextStrdup(cluster_mem, final->data);
		conn->cluster = cluster;
	}

	cluster->part_map[part_num] = conn;

	return conn;
}

/*
 * Fetch cluster version.
 * Called for each execution.
 */
static int
get_version(ProxyFunction *func, Datum dname)
{
	Datum		bin_val;
	bool		isnull;
	char		nulls[1];
	int			err;

	nulls[0] = (dname == (Datum) NULL) ? 'n' : ' ';

	err = SPI_execute_plan(version_plan, &dname, nulls, false, 0);
	if (err != SPI_OK_SELECT)
		plproxy_error(func, "get_version: spi error: %s",
					  SPI_result_code_string(err));
	if (SPI_processed != 1)
		plproxy_error(func, "get_version: got %d rows",
					  SPI_processed);

	bin_val = SPI_getbinval(SPI_tuptable->vals[0],
							SPI_tuptable->tupdesc, 1, &isnull);
	if (isnull)
		plproxy_error(func, "get_version: got NULL?");

	return DatumGetInt32(bin_val);
}

/* set a configuration option. */
static void
set_config_key(ProxyFunction *func, ProxyConfig *cf, const char *key, const char *val)
{
	if (pg_strcasecmp(key, "statement_timeout") == 0)
		/* ignore */ ;
	else if (pg_strcasecmp("connection_lifetime", key) == 0)
		cf->connection_lifetime = atoi(val);
	else if (pg_strcasecmp("query_timeout", key) == 0)
		cf->query_timeout = atoi(val);
	else if (pg_strcasecmp("disable_binary", key) == 0)
		cf->disable_binary = atoi(val);
	else if (pg_strcasecmp("keepalive_idle", key) == 0)
		cf->keepidle = atoi(val);
	else if (pg_strcasecmp("keepalive_interval", key) == 0)
		cf->keepintvl = atoi(val);
	else if (pg_strcasecmp("keepalive_count", key) == 0)
		cf->keepcnt = atoi(val);
	else
		plproxy_error(func, "Unknown config param: %s", key);
}

/*
 * Fetch cluster configuration.
 */
static int
get_config(ProxyCluster *cluster, Datum dname, ProxyFunction *func)
{
	int			err,
				i;
	TupleDesc	desc;
	const char *key,
			   *val;

	/* run query */
	err = SPI_execute_plan(config_plan, &dname, NULL, false, 0);
	if (err != SPI_OK_SELECT)
		plproxy_error(func, "fetch_config: spi error");

	/* check column types */
	desc = SPI_tuptable->tupdesc;
	if (desc->natts != 2)
		plproxy_error(func, "Cluster config must have 2 columns");
	if (SPI_gettypeid(desc, 1) != TEXTOID)
		plproxy_error(func, "Config column 1 must be text");
	if (SPI_gettypeid(desc, 2) != TEXTOID)
		plproxy_error(func, "Config column 2 must be text");

	/* fill values */
	for (i = 0; i < SPI_processed; i++)
	{
		HeapTuple	row = SPI_tuptable->vals[i];

		key = SPI_getvalue(row, desc, 1);
		if (key == NULL)
			plproxy_error(func, "key must not be NULL");

		val = SPI_getvalue(row, desc, 2);
		if (val == NULL)
			plproxy_error(func, "val must not be NULL");

		set_config_key(func, &cluster->config, key, val);
	}

	return 0;
}

/* allocate memory for cluster partitions */
static void
allocate_cluster_partitions(ProxyCluster *cluster, int nparts)
{
	MemoryContext old_ctx;

	/* free old one */
	if (cluster->conn_list)
		free_connlist(cluster);

	cluster->part_count = nparts;
	cluster->part_mask = cluster->part_count - 1;

	/* allocate lists */
	old_ctx = MemoryContextSwitchTo(cluster_mem);

	cluster->part_map = palloc0(nparts * sizeof(ProxyConnection *));
	cluster->conn_list = palloc0(nparts * sizeof(ProxyConnection));
	MemoryContextSwitchTo(old_ctx);
}

/* fetch list of parts */
static int
reload_parts(ProxyCluster *cluster, Datum dname, ProxyFunction *func)
{
	int			err,
				i;
	char	   *connstr;
	TupleDesc	desc;
	HeapTuple	row;

	/* run query */
	err = SPI_execute_plan(partlist_plan, &dname, NULL, false, 0);
	if (err != SPI_OK_SELECT)
		plproxy_error(func, "get_partlist: spi error");
	if (!check_valid_partcount(SPI_processed))
		plproxy_error(func, "get_partlist: invalid part count");

	/* check column types */
	desc = SPI_tuptable->tupdesc;
	if (desc->natts < 1)
		plproxy_error(func, "Partition config must have at least 1 columns");
	if (SPI_gettypeid(desc, 1) != TEXTOID)
		plproxy_error(func, "partition column 1 must be text");

	allocate_cluster_partitions(cluster, SPI_processed);

	/* fill values */
	for (i = 0; i < SPI_processed; i++)
	{
		row = SPI_tuptable->vals[i];

		connstr = SPI_getvalue(row, desc, 1);
		if (connstr == NULL)
			plproxy_error(func, "connstr must not be NULL");

		add_connection(cluster, connstr, i);
	}

	return 0;
}

#ifdef PLPROXY_USE_SQLMED

/* extract a partition number from foreign server option */
static bool
extract_part_num(const char *partname, int *part_num)
{
	char *partition_tags[] = { "p", "partition_", NULL };
	char **part_tag;
	char *errptr;

	for (part_tag = partition_tags; *part_tag; part_tag++)
	{
		if (strstr(partname, *part_tag) == partname)
		{
			*part_num = (int) strtoul(partname + strlen(*part_tag), &errptr, 10);
			if (*errptr == '\0')
				return true;
		}
	}

	return false;
}

/*
 * Validate single cluster option
 */
static void
validate_cluster_option(const char *name, const char *arg)
{
	const char **opt;

	/* see that a valid config option is specified */
	for (opt = cluster_config_options; *opt; opt++)
	{
		if (pg_strcasecmp(*opt, name) == 0)
			break;
	}

	if (*opt == NULL)
		elog(ERROR, "Pl/Proxy: invalid server option: %s", name);
	else if (strspn(arg, "0123456789") != strlen(arg))
		elog(ERROR, "Pl/Proxy: only integer options are allowed: %s=%s",
			 name, arg);
}

/*
 * Validate the generic option given to servers or user mappings defined with
 * plproxy foreign data wrapper.  Raise an ERROR if the option or its value is
 * considered invalid.
 */
Datum
plproxy_fdw_validator(PG_FUNCTION_ARGS)
{
	List	   *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);
	ListCell   *cell;
	int			part_count = 0;

	/* Pre 8.4.3 databases have broken validator interface, warn the user */
	if (catalog == InvalidOid)
	{
		ereport(NOTICE,
				(errcode(ERRCODE_WARNING),
				 errmsg("Pl/Proxy: foreign data wrapper validator disabled"),
				 errhint("validator is usable starting from PostgreSQL version 8.4.3")));

		PG_RETURN_BOOL(false);
	}

	foreach(cell, options_list)
	{
		DefElem    *def = lfirst(cell);
		char	   *arg = strVal(def->arg);
		int			part_num;

		if (catalog == ForeignServerRelationId)
		{
			if (extract_part_num(def->defname, &part_num))
			{
				/* partition definition */
				if (part_num != part_count)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("Pl/Proxy: partitions must be numbered consecutively"),
							 errhint("next valid partition number is %d", part_count)));
				++part_count;
			}
			else
			{
				validate_cluster_option(def->defname, arg);
			}
		}
		else if (catalog == UserMappingRelationId)
		{
			/* user mapping only accepts "user" and "password" */
			if (pg_strcasecmp(def->defname, "user") != 0 &&
				pg_strcasecmp(def->defname, "password") != 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("Pl/Proxy: invalid option to user mapping"),
						 errhint("valid options are \"user\" and \"password\"")));
			}
		}
		else if (catalog == ForeignDataWrapperRelationId)
		{
			validate_cluster_option(def->defname, arg);
		}
	}

	if (catalog == ForeignServerRelationId)
	{
		if (!check_valid_partcount(part_count))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("Pl/Proxy: invalid number of partitions"),
					 errhint("the number of partitions in a cluster must be power of 2 (attempted %d)", part_count)));
	}

	PG_RETURN_BOOL(true);
}

/*
 * Reload the cluster configuration and partitions from SQL/MED catalogs.
 */
static void
reload_sqlmed_cluster(ProxyFunction *func, ProxyCluster *cluster,
					  ForeignServer *foreign_server)
{
	UserMapping        *user_mapping;
	ForeignDataWrapper *fdw;
	HeapTuple			tup;
	AclResult			aclresult;
	StringInfo          user_options;
	ListCell		   *cell;
	Oid					userid;
	int					part_count = 0;
	int					part_num;


	userid = GetSessionUserId();
	user_mapping = GetUserMapping(userid, foreign_server->serverid);
	fdw = GetForeignDataWrapper(foreign_server->fdwid);

	/*
	 * Look up the server and user mapping TIDs for handling syscache invalidations.
	 */
    tup = SearchSysCache(FOREIGNSERVEROID,
						 ObjectIdGetDatum(foreign_server->serverid),
						 0, 0, 0);

    if (!HeapTupleIsValid(tup))
        elog(ERROR, "cache lookup failed for foreign server %u", foreign_server->serverid);

	scstamp_set(FOREIGNSERVEROID, &cluster->clusterStamp, tup);
	ReleaseSysCache(tup);

    tup = SearchSysCache(USERMAPPINGUSERSERVER,
						 ObjectIdGetDatum(user_mapping->userid),
						 ObjectIdGetDatum(foreign_server->serverid),
						 0, 0);

    if (!HeapTupleIsValid(tup))
	{
		/* Specific mapping not found, try PUBLIC */
		tup = SearchSysCache(USERMAPPINGUSERSERVER,
							 ObjectIdGetDatum(InvalidOid),
							 ObjectIdGetDatum(foreign_server->serverid),
							 0, 0);
		if (!HeapTupleIsValid(tup))
			elog(ERROR, "cache lookup failed for user mapping (%u,%u)",
				user_mapping->userid, foreign_server->serverid);
	}

	scstamp_set(USERMAPPINGOID, &cluster->umStamp, tup);

	ReleaseSysCache(tup);

	/*
	 * Check permissions, user must have usage on the server.
	 */
	aclresult = pg_foreign_server_aclcheck(foreign_server->serverid, userid, ACL_USAGE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_FOREIGN_SERVER, foreign_server->servername);

	/* Extract the common connect string elements from user mapping */
	user_options = makeStringInfo();
	foreach(cell, user_mapping->options)
	{
		DefElem    *def = lfirst(cell);

		appendStringInfo(user_options, "%s='%s' ", def->defname, strVal(def->arg));
	}

	/*
	 * Collect the configuration definitions from foreign data wrapper.
	 */
	foreach(cell, fdw->options)
	{
		DefElem    *def = lfirst(cell);

		set_config_key(func, &cluster->config, def->defname, strVal(def->arg));
	}

	/*
	 * Collect the cluster configuration and partition definitions from foreign
	 * server options. At first pass just collect the cluster options and count
	 * the number of partitions.
	 */
	foreach(cell, foreign_server->options)
	{
		DefElem    *def = lfirst(cell);

		if (extract_part_num(def->defname, &part_num))
		{
			if (part_num != part_count)
				plproxy_error(func, "partitions numbers must be consecutive");

			part_count++;
		}
		else
			set_config_key(func, &cluster->config, def->defname, strVal(def->arg));
	}

	if (!check_valid_partcount(part_count))
		plproxy_error(func, "invalid partition count");

	/*
	 * Now that the partition count is known, allocate the partitions and make
	 * a second pass over the options adding each connstr to cluster.
	 */
	allocate_cluster_partitions(cluster, part_count);

	foreach(cell, foreign_server->options)
	{
		DefElem    *def = lfirst(cell);
		StringInfo  buf = makeStringInfo();

		if (!extract_part_num(def->defname, &part_num))
			continue;

		appendStringInfo(buf, "%s%s%s", strVal(def->arg),
				user_options->len ? " " : "",
				user_options->data);
		add_connection(cluster, buf->data, part_num);
	}
}

/*
 * We have no SQL/MED definition for the cluster, determine if the
 * cluster is defined using the compat functions. Raise an ERROR
 * if the plproxy schema functions don't exist.
 */
static void
determine_compat_mode(ProxyCluster *cluster)
{
	bool		have_compat = false;
	HeapTuple	tup;

	/*
	 * See that we have plproxy schema and all the necessary functions
	 */

	tup = SearchSysCache(NAMESPACENAME, PointerGetDatum("plproxy"), 0, 0, 0);
	if (HeapTupleIsValid(tup))
	{
		Oid 		namespaceId = HeapTupleGetOid(tup);
		Oid			paramOids[] = { TEXTOID };
		oidvector	*parameterTypes = buildoidvector(paramOids, 1);
		const char	**funcname;

		/* All of the functions required to run pl/proxy in compat mode */
		static const char *compat_functions[] = {
			"get_cluster_version",
			"get_cluster_config",
			"get_cluster_partitions",
			NULL
		};

		for (funcname = compat_functions; *funcname; funcname++)
		{
			if (!SearchSysCacheExists(PROCNAMEARGSNSP,
									  PointerGetDatum(*funcname),
									  PointerGetDatum(parameterTypes),
									  ObjectIdGetDatum(namespaceId),
									  0))
				break;
		}

		/* we have the schema and all the functions - use compat */
		if (! *funcname)
			have_compat = true;

		ReleaseSysCache(tup);
	}

	if (!have_compat)
		elog(ERROR, "Pl/Proxy: cluster not found: %s", cluster->name);
}

/*
 * Syscache inval callback function for foreign servers and user mappings.
 *
 * Note: this invalidates compat clusters on any foreign server change. This
 * allows SQL/MED clusters to override those defined by plproxy schema
 * functions.
 */
static void
ClusterSyscacheCallback(Datum arg, int cacheid, SCInvalArg newStamp)
{
	ProxyCluster *cluster;

	for (cluster = cluster_list; cluster; cluster = cluster->next)
	{
		if (cluster->needs_reload)
		{
			/* already invalidated */
			continue;
		}
		else if (!cluster->sqlmed_cluster)
		{
			/* allow new SQL/MED servers to override compat definitions */
			if (cacheid == FOREIGNSERVEROID)
				cluster->needs_reload = true;
		}
		else if (cacheid == USERMAPPINGOID)
		{
			/* user mappings changed */
			if (scstamp_check(cacheid, &cluster->umStamp, newStamp))
				cluster->needs_reload = true;
		}
		else if (cacheid == FOREIGNSERVEROID)
		{
			/* server definitions changed */
			if (scstamp_check(cacheid, &cluster->clusterStamp, newStamp))
				cluster->needs_reload = true;
		}
	}
}

#endif

/*
 * Register syscache invalidation callbacks for SQL/MED clusters.
 */
void
plproxy_syscache_callback_init(void)
{
#ifdef PLPROXY_USE_SQLMED
	CacheRegisterSyscacheCallback(FOREIGNSERVEROID, ClusterSyscacheCallback, (Datum) 0);
	CacheRegisterSyscacheCallback(USERMAPPINGOID, ClusterSyscacheCallback, (Datum) 0);
#endif
}

/*
 * Reload the cluster configuration and partitions from plproxy.get_cluster*
 * functions.
 */
static void
reload_plproxy_cluster(ProxyFunction *func, ProxyCluster *cluster)
{
	Datum 	dname = DirectFunctionCall1(textin, CStringGetDatum(cluster->name));
	int		cur_version;

	plproxy_cluster_plan_init();

	/* fetch serial, also check if exists */
	cur_version = get_version(func, dname);

	/* update if needed */
	if (cur_version != cluster->version || cluster->needs_reload)
	{
		reload_parts(cluster, dname, func);
		get_config(cluster, dname, func);
		cluster->version = cur_version;
	}
}

/* allocate new cluster */
static ProxyCluster *
new_cluster(const char *name)
{
	ProxyCluster *cluster;
	MemoryContext old_ctx;

	old_ctx = MemoryContextSwitchTo(cluster_mem);

	cluster = palloc0(sizeof(*cluster));
	cluster->name = pstrdup(name);
	cluster->needs_reload = true;

	MemoryContextSwitchTo(old_ctx);

	return cluster;
}

/*
 * Refresh the cluster.
 */
static void
refresh_cluster(ProxyFunction *func, ProxyCluster *cluster)
{
#ifdef PLPROXY_USE_SQLMED
	if (cluster->needs_reload)
	{
		ForeignServer *server;

		/*
		 * Determine if this is a SQL/MED server name or a pl/proxy compat cluster.
		 * Fallback to plproxy.get_cluster*() functions if a foreign server is not
		 * found.
		 */
		server = GetForeignServerByName(cluster->name, true);
		cluster->sqlmed_cluster = (server != NULL);

		if (!cluster->sqlmed_cluster)
			determine_compat_mode(cluster);
		else
			reload_sqlmed_cluster(func, cluster, server);
	}
#endif

	/* Either no SQL/MED support or no such foreign server */
	if (!cluster->sqlmed_cluster)
		reload_plproxy_cluster(func, cluster);

	cluster->needs_reload = false;
}

/*
 * Get cached or create new fake cluster.
 */
static ProxyCluster *
fake_cluster(ProxyFunction *func, const char *connect_str)
{
	ProxyCluster *cluster;
	ProxyConnection *conn;
	MemoryContext old_ctx;

	/* search if cached */
	for (cluster = fake_cluster_list; cluster; cluster = cluster->next)
	{
		if (strcmp(cluster->name, connect_str) == 0)
			break;
	}

	if (cluster)
		return cluster;

	/* create if not */

	old_ctx = MemoryContextSwitchTo(cluster_mem);

	cluster = palloc0(sizeof(*cluster));
	cluster->name = pstrdup(connect_str);
	cluster->version = 1;
	cluster->part_count = 1;
	cluster->part_mask = 0;
	cluster->conn_count = 1;
	cluster->part_map = palloc(sizeof(ProxyConnection *));
	cluster->conn_list = palloc0(sizeof(ProxyConnection));
	conn = &cluster->conn_list[0];
	conn->cluster = cluster;
	cluster->part_map[0] = conn;

	conn->connstr = pstrdup(cluster->name);
	conn->state = C_NONE;

	MemoryContextSwitchTo(old_ctx);

	cluster->next = fake_cluster_list;
	fake_cluster_list = cluster;

	return cluster;
}

/*
 * Call resolve function
 */
static const char *
resolve_query(ProxyFunction *func, FunctionCallInfo fcinfo, ProxyQuery *query)
{
	const char *name;
	HeapTuple	row;
	TupleDesc	desc;

	plproxy_query_exec(func, fcinfo, query, NULL, 0);

	if (SPI_processed != 1)
		plproxy_error(func, "'%s' returned %d rows, expected 1",
					  query->sql, SPI_processed);

	desc = SPI_tuptable->tupdesc;
	if (SPI_gettypeid(desc, 1) != TEXTOID)
		plproxy_error(func, "expected text");

	row = SPI_tuptable->vals[0];
	name = SPI_getvalue(row, desc, 1);
	if (name == NULL)
		plproxy_error(func, "Cluster/connect name map func returned NULL");

	return name;
}

/*
 * Find cached cluster of create new one.
 *
 * Function argument is only for error handling.
 * Just func->cluster_name is used.
 */
ProxyCluster *
plproxy_find_cluster(ProxyFunction *func, FunctionCallInfo fcinfo)
{
	ProxyCluster *cluster;
	const char *name;


	/* functions used CONNECT with query */
	if (func->connect_sql) {
		const char *cstr;
		cstr = resolve_query(func, fcinfo, func->connect_sql);
		return fake_cluster(func, cstr);
	}


	/* functions used straight CONNECT */
	if (func->connect_str)
		return fake_cluster(func, func->connect_str);

	/* Cluster statement, either a lookup function or a name */
	if (func->cluster_sql)
		name = resolve_query(func, fcinfo, func->cluster_sql);
	else
		name = func->cluster_name;

	/* search if cached */
	for (cluster = cluster_list; cluster; cluster = cluster->next)
	{
		if (strcmp(cluster->name, name) == 0)
			break;
	}

	/* create if not */
	if (!cluster)
	{
		cluster = new_cluster(name);
		cluster->next = cluster_list;
		cluster_list = cluster;
	}

	/* determine cluster type, reload parts if necessary */
	refresh_cluster(func, cluster);

	return cluster;
}

static void
clean_cluster(ProxyCluster *cluster, struct timeval * now)
{
	ProxyConnection *conn;
	ProxyConfig *cf = &cluster->config;
	time_t		age;
	int			i;
	bool		drop;

	for (i = 0; i < cluster->conn_count; i++)
	{
		conn = &cluster->conn_list[i];
		if (conn->res)
		{
			PQclear(conn->res);
			conn->res = NULL;
		}
		if (!conn->db)
			continue;

		drop = false;
		if (PQstatus(conn->db) != CONNECTION_OK)
		{
			drop = true;
		}
		else if (cf->connection_lifetime <= 0)
		{
			/* no aging */
		}
		else
		{
			age = now->tv_sec - conn->connect_time;
			if (age >= cf->connection_lifetime)
				drop = true;
		}

		if (drop)
		{
			PQfinish(conn->db);
			conn->db = NULL;
			conn->state = C_NONE;
		}
	}
}

/*
 * Clean old connections and results from all clusters.
 */
void
plproxy_cluster_maint(struct timeval * now)
{
	ProxyCluster *cluster;

	for (cluster = cluster_list; cluster; cluster = cluster->next)
		clean_cluster(cluster, now);
	for (cluster = fake_cluster_list; cluster; cluster = cluster->next)
		clean_cluster(cluster, now);
}
