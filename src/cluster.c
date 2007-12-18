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
add_connection(ProxyCluster *cluster, char *connstr)
{
	int			i;
	ProxyConnection *conn;
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
	for (i = 0; i < cluster->conn_count; i++)
	{
		conn = &cluster->conn_list[i];
		if (strcmp(conn->connstr, final->data) == 0)
			return conn;
	}

	/* add new connection */
	conn = &cluster->conn_list[cluster->conn_count++];
	conn->connstr = MemoryContextStrdup(cluster_mem, final->data);

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
	ProxyConfig *cf = &cluster->config;

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

		if (strcasecmp(key, "statement_timeout") == 0)
			/* ignore */ ;
		else if (strcasecmp("connection_lifetime", key) == 0)
			cf->connection_lifetime = atoi(val);
		else if (strcasecmp("query_timeout", key) == 0)
			cf->query_timeout = atoi(val);
		else if (strcasecmp("disable_binary", key) == 0)
			cf->disable_binary = atoi(val);
		else
			plproxy_error(func, "Unknown config param: %s", key);
	}

	return 0;
}

/* fetch list of parts */
static int
reload_parts(ProxyCluster *cluster, Datum dname, ProxyFunction *func)
{
	int			err,
				i;
	ProxyConnection *conn;
	char	   *connstr;
	MemoryContext old_ctx;
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

	/* free old one */
	if (cluster->conn_list)
		free_connlist(cluster);

	cluster->part_count = SPI_processed;
	cluster->part_mask = cluster->part_count - 1;

	/* allocate lists */
	old_ctx = MemoryContextSwitchTo(cluster_mem);
	cluster->part_map = palloc0(SPI_processed * sizeof(ProxyConnection *));
	cluster->conn_list = palloc0(SPI_processed * sizeof(ProxyConnection));
	MemoryContextSwitchTo(old_ctx);

	/* fill values */
	for (i = 0; i < SPI_processed; i++)
	{
		row = SPI_tuptable->vals[i];

		connstr = SPI_getvalue(row, desc, 1);
		if (connstr == NULL)
			plproxy_error(func, "connstr must not be NULL");

		conn = add_connection(cluster, connstr);
		cluster->part_map[i] = conn;
	}

	return 0;
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

	MemoryContextSwitchTo(old_ctx);

	return cluster;
}

/*
 * Get cached or create new fake cluster.
 */
static ProxyCluster *
fake_cluster(ProxyFunction *func)
{
	ProxyCluster *cluster;
	ProxyConnection *conn;
	MemoryContext old_ctx;

	/* search if cached */
	for (cluster = fake_cluster_list; cluster; cluster = cluster->next)
	{
		if (strcmp(cluster->name, func->connect_str) == 0)
			break;
	}

	if (cluster)
		return cluster;

	/* create if not */

	old_ctx = MemoryContextSwitchTo(cluster_mem);

	cluster = palloc0(sizeof(*cluster));
	cluster->name = pstrdup(func->connect_str);
	cluster->version = 1;
	cluster->part_count = 1;
	cluster->part_mask = 0;
	cluster->conn_count = 1;
	cluster->part_map = palloc(sizeof(ProxyConnection *));
	cluster->conn_list = palloc0(sizeof(ProxyConnection));
	conn = &cluster->conn_list[0];
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
cluster_resolve_name(ProxyFunction *func, FunctionCallInfo fcinfo)
{
	const char *name;
	HeapTuple	row;
	TupleDesc	desc;

	plproxy_query_exec(func, fcinfo, func->cluster_sql);

	if (SPI_processed != 1)
		plproxy_error(func, "'%s' returned %d rows, expected 1",
					  func->cluster_sql->sql, SPI_processed);

	desc = SPI_tuptable->tupdesc;
	if (SPI_gettypeid(desc, 1) != TEXTOID)
		plproxy_error(func, "expected text");

	row = SPI_tuptable->vals[0];
	name = SPI_getvalue(row, desc, 1);
	if (name == NULL)
		plproxy_error(func, "Cluster name map func returned NULL");

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
	int			cur_version;
	const char *name;
	Datum		dname;

	/* functions used CONNECT */
	if (func->connect_str)
		return fake_cluster(func);

	/* initialize plans on demand only */
	plproxy_cluster_plan_init();

	if (func->cluster_sql)
		name = cluster_resolve_name(func, fcinfo);
	else
		name = func->cluster_name;

	/* create Datum for name */
	dname = DirectFunctionCall1(textin, CStringGetDatum(name));

	/* fetch serial, also check if exists */
	cur_version = get_version(func, dname);

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

	/* update if needed */
	if (cur_version != cluster->version)
	{
		reload_parts(cluster, dname, func);
		get_config(cluster, dname, func);
		cluster->version = cur_version;
	}

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
