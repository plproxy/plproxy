/*
 * PL/Proxy - easy access to partitioned database.
 *
 * Copyright (c) 2006-2020 PL/Proxy Authors
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
 * Data structures for PL/Proxy function handler.
 */

#ifndef plproxy_h_included
#define plproxy_h_included

#include <libpq-fe.h>

#include <postgres.h>
#include <funcapi.h>
#include <fmgr.h>
#include <executor/spi.h>

#include <foreign/foreign.h>
#include <catalog/pg_foreign_data_wrapper.h>
#include <catalog/pg_foreign_server.h>
#include <catalog/pg_user_mapping.h>

#include <access/hash.h>
#include <access/htup_details.h>
#include <access/reloptions.h>
#include <access/tupdesc.h>
#include <catalog/pg_namespace.h>
#include <catalog/pg_proc.h>
#include <catalog/pg_type.h>
#include <commands/trigger.h>
#include <lib/stringinfo.h>
#include <mb/pg_wchar.h>
#include <miscadmin.h>
#include <nodes/value.h>
#include <utils/acl.h>
#include <utils/array.h>
#include <utils/builtins.h>
#include <utils/hsearch.h>
#include <utils/inval.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/syscache.h>

#include "aatree.h"
#include "rowstamp.h"

#if PG_VERSION_NUM < 90300
#error PL/Proxy requires 9.3+
#endif

/* give offset of a field inside struct */
#ifndef offsetof
#define offsetof(type, field) ((unsigned long)&(((type *)0)->field))
#endif

/* given pointer to field inside struct, return pointer to struct */
#ifndef container_of
#define container_of(ptr, type, field) ((type *)((char *)(ptr) - offsetof(type, field)))
#endif

/*
 * backwards compatibility with v10.
 */

#if PG_VERSION_NUM >= 110000
#define ACL_KIND_FOREIGN_SERVER OBJECT_FOREIGN_SERVER
#endif

/*
 * Determine if this argument is to SPLIT
 */
#define IS_SPLIT_ARG(func, arg)	((func)->split_args && (func)->split_args[arg])

/*
 * Maintenece period in seconds.  Connnections will be freed
 * from stale results, and checked for lifetime.
 */
#define PLPROXY_MAINT_PERIOD		(2*60)

/*
 * Check connections that are idle more than this many seconds.
 * Set 0 to always check.
 */
#define PLPROXY_IDLE_CONN_CHECK		2

/* Temp structure for query parsing */
typedef struct QueryBuffer QueryBuffer;

/*
 * Parsed query where references to function arguments
 * are replaced with local args numbered sequentially: $1..$n.
 */
typedef struct ProxyQuery
{
	char	   *sql;			/* Prepared SQL string */
	int			arg_count;		/* Argument count for ->sql */
	int		   *arg_lookup;		/* Maps local references to function args */
	void	   *plan;			/* Optional prepared plan for local queries */
} ProxyQuery;

/* Flag indicating where function should be executed */
typedef enum RunOnType
{
	R_HASH = 1,				/* partition(s) returned by hash function */
	R_ALL = 2,				/* on all partitions */
	R_ANY = 3,				/* decide randomly during runtime */
	R_EXACT = 4				/* exact part number */
} RunOnType;

/* Connection states for async handler */
typedef enum ConnState
{
	C_NONE = 0,					/* no connection object yet */
	C_CONNECT_WRITE,			/* login phase: sending data */
	C_CONNECT_READ,				/* login phase: waiting for server */
	C_READY,					/* connection ready for query */
	C_QUERY_WRITE,				/* query phase: sending data */
	C_QUERY_READ,				/* query phase: waiting for server */
	C_DONE,						/* query done, result available */
} ConnState;

/* Stores result from plproxy.get_cluster_config() */
typedef struct ProxyConfig
{
	int			connect_timeout;		/* How long connect may take (secs) */
	int			query_timeout;			/* How long query may take (secs) */
	int			connection_lifetime;	/* How long the connection may live (secs) */
	int			disable_binary;			/* Avoid binary I/O */
	int			modular_mapping;		/* Use modulus (%) instead masking (&) */
	char		default_user[NAMEDATALEN];
} ProxyConfig;

typedef struct ConnUserInfo {
	struct AANode node;
	Oid user_oid;

	char *username;
	char *extra_connstr;		/* user= and password= */

	SysCacheStamp umStamp;
	bool needs_reload;
} ConnUserInfo;

typedef struct ProxyConnectionState {
	struct AANode node;			/* node head in user->state tree */

	ConnUserInfo *userinfo;

	PGconn	   *db;				/* libpq connection handle */
	ConnState	state;			/* Connection state */
	time_t		connect_time;	/* When connection was started */
	time_t		query_time;		/* When last query was sent */
	bool		same_ver;		/* True if dest backend has same X.Y ver */
	bool		tuning;			/* True if tuning query is running on conn */
	bool		waitCancel;		/* True if waiting for answer from cancel */
} ProxyConnectionState;

/* Single database connection */
typedef struct ProxyConnection
{
	struct AANode node;

	struct ProxyCluster *cluster;
	const char *connstr;		/* Connection string for libpq */

	struct AATree userstate_tree; /* user->state tree */

	/* state */
	PGresult   *res;			/* last resultset */
	/*
	 * Maps result field num to libpq column num.
	 * NULL when scalar result.
	 */
	int		   *result_map;
	int			pos;			/* Current position inside res */
	ProxyConnectionState *cur;

	/*
	 * Nonzero if this connection should be used. The actual tag value is only
	 * used by SPLIT processing, others should treat it as a boolean value.
	 */
	int			run_tag;

	/*
	 * Per-connection parameters. These are a assigned just before the 
	 * remote call is made.
	 */

	Datum			   *split_params;					/* Split array parameters */
	ArrayBuildState	  **bstate;							/* Temporary build state */
	ProxyQuery  	   *remote_sql;							/* Query to execute */
	const char		   *param_values[FUNC_MAX_ARGS];	/* Parameter values */
	int					param_lengths[FUNC_MAX_ARGS];	/* Parameter lengths (binary io) */
	int					param_formats[FUNC_MAX_ARGS];	/* Parameter formats (binary io) */
} ProxyConnection;

/* Info about one cluster */
typedef struct ProxyCluster
{
	struct AANode node;			/* Node in name => cluster lookup tree */

	const char *name;			/* Cluster name */
	int			version;		/* Cluster version */
	ProxyConfig config;			/* Cluster config */

	int			part_count;		/* Number of partitions - power of 2 */
	int			part_mask;		/* Mask to use to get part number from hash */
	ProxyConnection **part_map; /* Pointers to ProxyConnections */

	int active_count;			/* number of active connections */
	ProxyConnection **active_list; /* active ProxyConnection in current query */

	struct AATree conn_tree;	/* connstr -> ProxyConnection */

	struct AATree userinfo_tree; /* username->userinfo tree */
	ConnUserInfo *cur_userinfo;	/* userinfo struct for current request */

	int			ret_cur_conn;	/* Result walking: index of current conn */
	int			ret_cur_pos;	/* Result walking: index of current row */
	int			ret_total;		/* Result walking: total rows left */

	Oid			sqlmed_server_oid;

	bool		fake_cluster;	/* single connect-string cluster */
	bool		sqlmed_cluster;	/* True if the cluster is defined using SQL/MED */
	bool		needs_reload;	/* True if the cluster partition list should be reloaded */
	bool		busy;			/* True if the cluster is already involved in execution */

	/*
	 * SQL/MED clusters: TIDs of the foreign server and user mapping catalog tuples.
	 * Used in to perform cluster invalidation in syscache callbacks.
	 */
	SysCacheStamp clusterStamp;

	/* notice processing: provide info about currently executing function */
	struct ProxyFunction	*cur_func;
} ProxyCluster;

/*
 * Type info cache.
 *
 * As the decision to send/receive binary may
 * change in runtime, both text and binary
 * function calls must be cached.
 */
typedef struct ProxyType
{
	char	   *name;			/* Name of the type */
	Oid			type_oid;		/* Oid of the type */

	Oid			io_param;		/* Extra arg for input_func */
	bool		for_send;		/* True if for outputting */
	bool		has_send;		/* Has binary output */
	bool		has_recv;		/* Has binary input */
	bool		by_value;		/* False if Datum is a pointer to data */
	char		alignment;		/* Type alignment */
	bool		is_array;		/* True if array */
	Oid			elem_type_oid;	/* Array element type oid */
	struct ProxyType *elem_type_t;	/* Elem type info, filled lazily */
	short		length;			/* Type length */

	/* I/O functions */
	union
	{
		struct
		{
			FmgrInfo	output_func;
			FmgrInfo	send_func;
		}			out;
		struct
		{
			FmgrInfo	input_func;
			FmgrInfo	recv_func;
		}			in;
	}			io;
} ProxyType;

/*
 * Info cache for composite return type.
 *
 * There is AttInMetadata in core, but it does not support
 * binary receive, so need our own struct.
 */
typedef struct ProxyComposite
{
	TupleDesc	tupdesc;		/* Return tuple descriptor */
	ProxyType **type_list;		/* Column type info */
	char	  **name_list;		/* Quoted column names */
	int			nfields;		/* number of non-dropped fields */
	bool		use_binary;		/* True if all columns support binary recv */
	bool		alterable;		/* if it's real table that can change */
	RowStamp	stamp;
} ProxyComposite;

/*
 * Deconstructed array parameters
 */
typedef struct DatumArray
{
	ProxyType  *type;
	Datum	   *values;
	bool	   *nulls;
	int			elem_count;
} DatumArray;

/*
 * Complete info about compiled function.
 *
 * Note: only IN and INOUT arguments are cached here.
 */
typedef struct ProxyFunction
{
	const char *name;			/* Fully-qualified and quoted function name */
	Oid			oid;			/* Function OID */
	MemoryContext ctx;			/* Where runtime allocations should happen */
	MemoryContext tuplectx;		/* short-lived memory for tuple creation */

	RowStamp	stamp;			/* for pg_proc cache validation */

	ProxyType **arg_types;		/* Info about arguments */
	char	  **arg_names;		/* Argument names, may contain NULLs */
	short		arg_count;		/* Argument count of proxy function */

	bool	   *split_args;		/* Map of arguments to split */

	bool		retset;			/* set returning function */

	/* if the function returns untyped RECORD that needs AS clause */
	bool		dynamic_record;

	/* One of them is defined, other NULL */
	ProxyType  *ret_scalar;		/* Type info for scalar return val */
	ProxyComposite *ret_composite;	/* Type info for composite return val */

	/* data from function body */
	const char *cluster_name;	/* Cluster where function should run */
	ProxyQuery *cluster_sql;	/* Optional query for name resolving */

	RunOnType	run_type;		/* Run type */
	ProxyQuery *hash_sql;		/* Hash execution for R_HASH */
	int			exact_nr;		/* Hash value for R_EXACT */
	const char *connect_str;	/* libpq string for CONNECT function */
	ProxyQuery *connect_sql;	/* Optional query for CONNECT function */
	const char *target_name;	/* Optional target function name */

	bool	is_execute;
	int		execute_arg;
	bool	execute_is_array;

	/*
	 * calculated data
	 */

	ProxyQuery *remote_sql;		/* query to be run repotely */

	/*
	 * current execution data
	 */

	/*
	 * Cluster to be executed on.  In case of CONNECT,
	 * function's private fake cluster object.
	 */
	ProxyCluster *cur_cluster;
} ProxyFunction;

/* main.c */
Datum		plproxy_call_handler(PG_FUNCTION_ARGS);
Datum		plproxy_validator(PG_FUNCTION_ARGS);
void		plproxy_error_with_state(ProxyFunction *func, int sqlstate, const char *fmt, ...)
	__attribute__((format(PG_PRINTF_ATTRIBUTE, 3, 4)));
void		plproxy_remote_error(ProxyFunction *func, ProxyConnection *conn, const PGresult *res, bool iserr);
#define plproxy_error(func,...) plproxy_error_with_state((func), ERRCODE_INTERNAL_ERROR, __VA_ARGS__)

/* function.c */
void		plproxy_function_cache_init(void);
void	   *plproxy_func_alloc(ProxyFunction *func, int size);
char	   *plproxy_func_strdup(ProxyFunction *func, const char *s);
int			plproxy_get_parameter_index(ProxyFunction *func, const char *ident);
bool		plproxy_split_add_ident(ProxyFunction *func, const char *ident);
void		plproxy_split_all_arrays(ProxyFunction *func);
bool		plproxy_execute_ident(ProxyFunction *func, const char *ident);
ProxyFunction *plproxy_compile_and_cache(FunctionCallInfo fcinfo);
ProxyFunction *plproxy_compile(FunctionCallInfo fcinfo, HeapTuple proc_tuple, bool validate_only);

/* execute.c */
void		plproxy_exec(ProxyFunction *func, FunctionCallInfo fcinfo);
void		plproxy_clean_results(ProxyCluster *cluster);
void		plproxy_disconnect(ProxyConnectionState *cur);

/* scanner.c */
int			plproxy_yyget_lineno(void);
int			plproxy_yylex_destroy(void);
int			plproxy_yylex(void);
void		plproxy_scanner_sqlmode(bool val);
void		plproxy_yylex_startup(void);

/* parser.y */
void		plproxy_run_parser(ProxyFunction *func, const char *body, int len);
void		plproxy_yyerror(const char *fmt, ...)
	__attribute__((format(PG_PRINTF_ATTRIBUTE, 1, 2)));

/* type.c */
ProxyComposite *plproxy_composite_info(ProxyFunction *func, TupleDesc tupdesc);
ProxyType  *plproxy_find_type_info(ProxyFunction *func, Oid oid, bool for_send);
ProxyType  *plproxy_get_elem_type(ProxyFunction *func, ProxyType *type, bool for_send);
char	   *plproxy_send_type(ProxyType *type, Datum val, bool allow_bin, int *len, int *fmt);
Datum		plproxy_recv_type(ProxyType *type, char *str, int len, bool bin);
HeapTuple	plproxy_recv_composite(ProxyComposite *meta, char **values, int *lengths, int *fmts);
void		plproxy_free_type(ProxyType *type);
void		plproxy_free_composite(ProxyComposite *meta);
bool		plproxy_composite_valid(ProxyComposite *type);

/* cluster.c */
void		plproxy_cluster_cache_init(void);
void		plproxy_syscache_callback_init(void);
ProxyCluster *plproxy_find_cluster(ProxyFunction *func, FunctionCallInfo fcinfo);
void		plproxy_cluster_maint(struct timeval * now);
void		plproxy_activate_connection(struct ProxyConnection *conn);
void		plproxy_append_cstr_option(StringInfo cstr, const char *name, const char *val);
void	   *plproxy_allocate_memory(size_t size);

/* result.c */
Datum		plproxy_result(ProxyFunction *func, FunctionCallInfo fcinfo);
HeapTuple	plproxy_tuple_from_result(PGresult *res, TupleDesc tupdesc, ProxyFunction *func, ProxyConnection *conn);

/* query.c */
QueryBuffer *plproxy_query_start(ProxyFunction *func, bool add_types);
bool		plproxy_query_add_const(QueryBuffer *q, const char *data);
bool		plproxy_query_add_ident(QueryBuffer *q, const char *ident);
ProxyQuery *plproxy_query_finish(QueryBuffer *q);
ProxyQuery *plproxy_standard_query(ProxyFunction *func, bool add_types);
void		plproxy_query_prepare(ProxyFunction *func, FunctionCallInfo fcinfo, ProxyQuery *q, bool split_support);
void		plproxy_query_exec(ProxyFunction *func, FunctionCallInfo fcinfo, ProxyQuery *q,
							   DatumArray **array_params, int array_row);
void		plproxy_query_freeplan(ProxyQuery *q);

#endif
