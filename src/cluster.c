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
 * Tree of clusters.
 *
 * For searching by name.
 */
static struct AATree cluster_tree;

/*
 * Similar list for fake clusters (for CONNECT functions).
 *
 * Cluster name will be actual connect string.
 */
static struct AATree fake_cluster_tree;

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

static int cluster_name_cmp(uintptr_t val, struct AANode *node)
{
	const char *name = (const char *)val;
	const ProxyCluster *cluster = container_of(node, ProxyCluster, node);

	return strcmp(name, cluster->name);
}

static int conn_cstr_cmp(uintptr_t val, struct AANode *node)
{
	const char *name = (const char *)val;
	const ProxyConnection *conn = container_of(node, ProxyConnection, node);

	return strcmp(name, conn->connstr);
}

static void conn_free(struct AANode *node, void *arg)
{
	ProxyConnection *conn = container_of(node, ProxyConnection, node);

	aatree_destroy(&conn->userstate_tree);
	if (conn->res)
		PQclear(conn->res);
	pfree(conn);
}

static int state_user_cmp(uintptr_t val, struct AANode *node)
{
	const char *name = (const char *)val;
	const ProxyConnectionState *state = container_of(node, ProxyConnectionState, node);

	return strcmp(name, state->userinfo->username);
}

static void state_free(struct AANode *node, void *arg)
{
	ProxyConnectionState *state = container_of(node, ProxyConnectionState, node);

	plproxy_disconnect(state);
	memset(state, 0, sizeof(*state));
	pfree(state);
}

static int userinfo_cmp(uintptr_t val, struct AANode *node)
{
	const char *name = (const char *)val;
	const ConnUserInfo *info = container_of(node, ConnUserInfo, node);

	return strcmp(name, info->username);
}

static void userinfo_free(struct AANode *node, void *arg)
{
	ConnUserInfo *info = container_of(node, ConnUserInfo, node);
	pfree(info->username);
	if (info->extra_connstr)
	{
		memset(info->extra_connstr, 0, strlen(info->extra_connstr));
		pfree(info->extra_connstr);
	}
	memset(info, 0, sizeof(*info));
	pfree(info);
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
	aatree_init(&cluster_tree, cluster_name_cmp, NULL);
	aatree_init(&fake_cluster_tree, cluster_name_cmp, NULL);
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
	aatree_destroy(&cluster->conn_tree);

	pfree(cluster->part_map);
	pfree(cluster->active_list);

	cluster->part_map = NULL;
	cluster->part_count = 0;
	cluster->part_mask = 0;
	cluster->active_count = 0;
}

/*
 * Add new database connection if it does not exists.
 */
static void
add_connection(ProxyCluster *cluster, const char *connstr, int part_num)
{
	struct AANode *node;
	ProxyConnection *conn = NULL;

	/* check if already have it */
	node = aatree_search(&cluster->conn_tree, (uintptr_t)connstr);
	if (node)
		conn = container_of(node, ProxyConnection, node);

	/* add new connection */
	if (!conn)
	{
		conn = MemoryContextAllocZero(cluster_mem, sizeof(ProxyConnection));
		conn->connstr = MemoryContextStrdup(cluster_mem, connstr);
		conn->cluster = cluster;

		aatree_init(&conn->userstate_tree, state_user_cmp, state_free);

		aatree_insert(&cluster->conn_tree, (uintptr_t)connstr, &conn->node);
	}

	cluster->part_map[part_num] = conn;
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

/* forget old values */
static void
clear_config(ProxyConfig *cf)
{
	memset(cf, 0, sizeof(*cf));
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
	else if (pg_strcasecmp("default_user", key) == 0)
		snprintf(cf->default_user, sizeof(cf->default_user), "%s", val);
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

	clear_config(&cluster->config);

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
	if (cluster->part_map)
		free_connlist(cluster);

	cluster->part_count = nparts;
	cluster->part_mask = cluster->part_count - 1;

	/* allocate lists */
	old_ctx = MemoryContextSwitchTo(cluster_mem);
	cluster->part_map = palloc0(nparts * sizeof(ProxyConnection *));
	cluster->active_list = palloc0(nparts * sizeof(ProxyConnection *));
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

static void
reload_sqlmed_user(ProxyFunction *func, ProxyCluster *cluster)
{
	ConnUserInfo *userinfo = cluster->cur_userinfo;

	UserMapping        *um;
	HeapTuple			tup;
	StringInfoData      cstr;
	ListCell		   *cell;
	AclResult			aclresult;
	bool				got_user;


	um = GetUserMapping(userinfo->user_oid, cluster->sqlmed_server_oid);

	/* retry same lookup so we can set cache stamp... */
    tup = SearchSysCache(USERMAPPINGUSERSERVER,
						 ObjectIdGetDatum(um->userid),
						 ObjectIdGetDatum(um->serverid),
						 0, 0);
    if (!HeapTupleIsValid(tup))
	{
		/* Specific mapping not found, try PUBLIC */
		tup = SearchSysCache(USERMAPPINGUSERSERVER,
							 ObjectIdGetDatum(InvalidOid),
							 ObjectIdGetDatum(um->serverid),
							 0, 0);
		if (!HeapTupleIsValid(tup))
			elog(ERROR, "cache lookup failed for user mapping (%u,%u)",
				um->userid, um->serverid);
	}
	scstamp_set(USERMAPPINGOID, &userinfo->umStamp, tup);
	ReleaseSysCache(tup);

	/*
	 * Check permissions, user must have usage on the server.
	 */
	aclresult = pg_foreign_server_aclcheck(um->serverid, um->userid, ACL_USAGE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_FOREIGN_SERVER, cluster->name);

	/* Extract the common connect string elements from user mapping */
	got_user = false;
	initStringInfo(&cstr);
	foreach(cell, um->options)
	{
		DefElem    *def = lfirst(cell);

		if (strcmp(def->defname, "user") == 0)
			got_user = true;

		appendStringInfo(&cstr, " %s='%s'", def->defname, strVal(def->arg));
	}

	/* make sure we have 'user=' in connect string */
	if (!got_user)
		appendStringInfo(&cstr, " user='%s'", userinfo->username);

	/* free old string */
	if (userinfo->extra_connstr)
	{
		memset(userinfo->extra_connstr, 0, strlen(userinfo->extra_connstr));
		pfree(userinfo->extra_connstr);
		userinfo->extra_connstr = NULL;
	}

	/* set up new connect string */
	userinfo->extra_connstr = MemoryContextStrdup(cluster_mem, cstr.data);
	memset(cstr.data, 0, cstr.len);
	pfree(cstr.data);
}

/*
 * Reload the cluster configuration and partitions from SQL/MED catalogs.
 */
static void
reload_sqlmed_cluster(ProxyFunction *func, ProxyCluster *cluster,
					  ForeignServer *foreign_server)
{
	ConnUserInfo		*info = cluster->cur_userinfo;
	ForeignDataWrapper *fdw;
	HeapTuple			tup;
	AclResult			aclresult;
	ListCell		   *cell;
	int					part_count = 0;
	int					part_num;


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

	/*
	 * Check permissions, user must have usage on the server.
	 */
	aclresult = pg_foreign_server_aclcheck(foreign_server->serverid, info->user_oid, ACL_USAGE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_FOREIGN_SERVER, foreign_server->servername);

	/* drop old config values */
	clear_config(&cluster->config);

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

		if (!extract_part_num(def->defname, &part_num))
			continue;

		add_connection(cluster, strVal(def->arg), part_num);
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

static void inval_one_umap(struct AANode *n, void *arg)
{
	ConnUserInfo *info = container_of(n, ConnUserInfo, node);
	SCInvalArg newStamp;

	if (info->needs_reload)
		/* already invalidated */
		return;

	if (arg == NULL)
	{
		info->needs_reload = true;
		return;
	}

	newStamp = *(SCInvalArg *)arg;
	if (scstamp_check(USERMAPPINGOID, &info->umStamp, newStamp))
		/* user mappings changed */
		info->needs_reload = true;
}

static void inval_umapping(struct AANode *n, void *arg)
{
	ProxyCluster *cluster = container_of(n, ProxyCluster, node);

	aatree_walk(&cluster->userinfo_tree, AA_WALK_IN_ORDER, inval_one_umap, arg);
}

static void inval_fserver(struct AANode *n, void *arg)
{
	ProxyCluster *cluster = container_of(n, ProxyCluster, node);
	SCInvalArg newStamp = *(SCInvalArg *)arg;

	if (cluster->needs_reload)
		/* already invalidated */
		return;
	else if (!cluster->sqlmed_cluster)
		/* allow new SQL/MED servers to override compat definitions */
		cluster->needs_reload = true;
	else if (scstamp_check(FOREIGNSERVEROID, &cluster->clusterStamp, newStamp))
		/* server definitions changed */
		cluster->needs_reload = true;

	/* tag all users too */
	if (cluster->needs_reload)
		inval_umapping(&cluster->node, NULL);
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
	if (cacheid == FOREIGNSERVEROID)
		aatree_walk(&cluster_tree, AA_WALK_IN_ORDER, inval_fserver, &newStamp);
	else if (cacheid == USERMAPPINGOID)
		aatree_walk(&cluster_tree, AA_WALK_IN_ORDER, inval_umapping, &newStamp);
}

/*
 * Register syscache invalidation callbacks for SQL/MED clusters.
 */
void
plproxy_syscache_callback_init(void)
{
	CacheRegisterSyscacheCallback(FOREIGNSERVEROID, ClusterSyscacheCallback, (Datum) 0);
	CacheRegisterSyscacheCallback(USERMAPPINGOID, ClusterSyscacheCallback, (Datum) 0);
}

#else /* !PLPROXY_USE_SQLMED */

void plproxy_syscache_callback_init(void) {}

#endif



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

	aatree_init(&cluster->conn_tree, conn_cstr_cmp, conn_free);
	aatree_init(&cluster->userinfo_tree, userinfo_cmp, userinfo_free);

	MemoryContextSwitchTo(old_ctx);

	return cluster;
}

/*
 * Invalidate all connections for particular user
 */
#ifdef PLPROXY_USE_SQLMED

static void inval_userinfo_state(struct AANode *node, void *arg)
{
	ProxyConnectionState *cur = container_of(node, ProxyConnectionState, node);
	ConnUserInfo *userinfo = arg;

	if (cur->userinfo == userinfo && cur->db)
		plproxy_disconnect(cur);
}

static void inval_userinfo_conn(struct AANode *node, void *arg)
{
	ProxyConnection *conn = container_of(node, ProxyConnection, node);
	ConnUserInfo *userinfo = arg;

	aatree_walk(&conn->userstate_tree, AA_WALK_IN_ORDER, inval_userinfo_state, userinfo);
}

static void inval_user_connections(ProxyCluster *cluster, ConnUserInfo *userinfo)
{
	/* find all connections with this user and drop them */
	aatree_walk(&cluster->conn_tree, AA_WALK_IN_ORDER, inval_userinfo_conn, userinfo);

	/*
	 * We can clear the flag only when it's certain
	 * that no connections with old info exist
	 */
	userinfo->needs_reload = false;
}

#endif

/*
 * Initialize user info struct
 */

static ConnUserInfo *
get_userinfo(ProxyCluster *cluster, Oid user_oid)
{
	ConnUserInfo *userinfo;
	struct AANode *node;
	const char *username;

	username = GetUserNameFromId(user_oid);

	node = aatree_search(&cluster->userinfo_tree, (uintptr_t)username);
	if (node) {
		userinfo = container_of(node, ConnUserInfo, node);
	} else {
		userinfo = MemoryContextAllocZero(cluster_mem, sizeof(*userinfo));
		userinfo->username = MemoryContextStrdup(cluster_mem, username);

		aatree_insert(&cluster->userinfo_tree, (uintptr_t)username, &userinfo->node);
	}

	if (userinfo->user_oid != user_oid)
	{
		/* user got renamed? */
		userinfo->user_oid = user_oid;
		userinfo->needs_reload = true;
	}

	return userinfo;
}

#if PG_VERSION_NUM < 90100

static Oid
get_role_oid(const char *rolname, bool missing_ok)
{
	Oid         oid;

	oid = GetSysCacheOid(AUTHNAME, CStringGetDatum(rolname), 0, 0, 0);
	if (!OidIsValid(oid) && !missing_ok)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("role \"%s\" does not exist", rolname)));
	return oid;
}

#endif

/*
 * Refresh the cluster.
 */
static void
refresh_cluster(ProxyFunction *func, ProxyCluster *cluster)
{
	ConnUserInfo *uinfo;
	ProxyConfig *cf = &cluster->config;
	Oid user_oid = InvalidOid;

	/*
	 * Decide which user to use for connections.
	 */
	if (cf->default_user[0])
	{
		if (strcmp(cf->default_user, "session_user") == 0)
			user_oid = GetSessionUserId();
		else if (strcmp(cf->default_user, "current_user") == 0)
			user_oid = GetUserId();
		else if (1)
			/* dont support custom users, seems unnecessary */
			elog(ERROR, "default_user: Expect 'current_user' or 'session_user', got '%s'",
				 cf->default_user);
		else
			/* easy to support, but seems confusing conceptually */
			user_oid = get_role_oid(cf->default_user, false);
	}
	else
	{
		/* default: current_user */
		user_oid = GetUserId();
	}

	/* set up user cache */
	uinfo = get_userinfo(cluster, user_oid);
	cluster->cur_userinfo = uinfo;

	/* SQL/MED server reload */
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
		{
			cluster->sqlmed_server_oid = server->serverid;
			reload_sqlmed_cluster(func, cluster, server);
		}
	}

#endif

	/* SQL/MED user reload */
	if (uinfo->needs_reload)
	{
#ifdef PLPROXY_USE_SQLMED
		if (cluster->sqlmed_cluster)
		{
			inval_user_connections(cluster, uinfo);
			reload_sqlmed_user(func, cluster);
		}
		else
#endif
			uinfo->needs_reload = false;
	}

	/* old-style cluster reload */
	if (!cluster->sqlmed_cluster && !cluster->fake_cluster)
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
	MemoryContext old_ctx;
	struct AANode *n;

	/* search if cached */
	n = aatree_search(&fake_cluster_tree, (uintptr_t)connect_str);
	if (n)
	{
		cluster = container_of(n, ProxyCluster, node);
		goto done;
	}

	/* create if not */
	cluster = new_cluster(connect_str);

	old_ctx = MemoryContextSwitchTo(cluster_mem);

	cluster->fake_cluster = true;
	cluster->version = 1;
	cluster->part_count = 1;
	cluster->part_mask = 0;
	cluster->part_map = palloc(cluster->part_count * sizeof(ProxyConnection *));
	cluster->active_list = palloc(cluster->part_count * sizeof(ProxyConnection *));

	MemoryContextSwitchTo(old_ctx);

	add_connection(cluster, connect_str, 0);

	aatree_insert(&fake_cluster_tree, (uintptr_t)connect_str, &cluster->node);

done:
	refresh_cluster(func, cluster);
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
	ProxyCluster *cluster = NULL;
	const char *name;
	struct AANode *node;


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
	node = aatree_search(&cluster_tree, (uintptr_t)name);
	if (node)
		cluster = container_of(node, ProxyCluster, node);

	/* create if not */
	if (!cluster)
	{
		cluster = new_cluster(name);
		cluster->needs_reload = true;
		aatree_insert(&cluster_tree, (uintptr_t)name, &cluster->node);
	}

	/* determine cluster type, reload parts if necessary */
	refresh_cluster(func, cluster);

	return cluster;
}

/*
 * Move connection to active list and init current
 * connection state.
 */
void plproxy_activate_connection(struct ProxyConnection *conn)
{
	ProxyCluster *cluster = conn->cluster;
	ConnUserInfo *userinfo = cluster->cur_userinfo;
	const char *username = userinfo->username;
	struct AANode *node;
	ProxyConnectionState *cur;

	/* move connection to active_list */
	cluster->active_list[cluster->active_count] = conn;
	cluster->active_count++;

	/* fill ->cur pointer */

	node = aatree_search(&conn->userstate_tree, (uintptr_t)username);
	if (node) {
		cur = container_of(node, ProxyConnectionState, node);
	} else {
		cur = MemoryContextAllocZero(cluster_mem, sizeof(*cur));
		cur->userinfo = userinfo;
		aatree_insert(&conn->userstate_tree, (uintptr_t)username, &cur->node);
	}
	conn->cur = cur;
}

/*
 * Clean old connections and results from all clusters.
 */

struct MaintInfo {
	struct ProxyConfig *cf;
	struct timeval *now;
};

static void clean_state(struct AANode *node, void *arg)
{
	ProxyConnectionState *cur = container_of(node, ProxyConnectionState, node);
	ConnUserInfo *uinfo = cur->userinfo;
	struct MaintInfo *maint = arg;
	ProxyConfig *cf = maint->cf;
	struct timeval *now = maint->now;
	time_t		age;
	bool		drop;

	if (!cur->db)
		return;

	drop = false;
	if (PQstatus(cur->db) != CONNECTION_OK)
	{
		drop = true;
	}
	else if (uinfo->needs_reload)
	{
		drop = true;
	}
	else if (cf->connection_lifetime <= 0)
	{
		/* no aging */
	}
	else
	{
		age = now->tv_sec - cur->connect_time;
		if (age >= cf->connection_lifetime)
			drop = true;
	}

	if (drop)
		plproxy_disconnect(cur);
}

static void clean_conn(struct AANode *node, void *arg)
{
	ProxyConnection *conn = container_of(node, ProxyConnection, node);
	struct MaintInfo *maint = arg;

	if (conn->res)
	{
		PQclear(conn->res);
		conn->res = NULL;
	}

	aatree_walk(&conn->userstate_tree, AA_WALK_IN_ORDER, clean_state, maint);
}

static void clean_cluster(struct AANode *n, void *arg)
{
	ProxyCluster *cluster = container_of(n, ProxyCluster, node);
	struct MaintInfo maint;

	maint.cf = &cluster->config;
	maint.now = arg;

	aatree_walk(&cluster->conn_tree, AA_WALK_IN_ORDER, clean_conn, &maint);
}

void
plproxy_cluster_maint(struct timeval * now)
{
	aatree_walk(&cluster_tree, AA_WALK_IN_ORDER, clean_cluster, now);
	aatree_walk(&fake_cluster_tree, AA_WALK_IN_ORDER, clean_cluster, now);
}

