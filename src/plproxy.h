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
 * Data structures for PL/Proxy function handler.
 */

#ifndef plproxy_h_included
#define plproxy_h_included

#include <postgres.h>
#include <funcapi.h>
#include <executor/spi.h>

#include <access/tupdesc.h>
#include <catalog/pg_namespace.h>
#include <catalog/pg_proc.h>
#include <catalog/pg_type.h>
#include <commands/trigger.h>
#include <mb/pg_wchar.h>
#include <miscadmin.h>
#include <utils/builtins.h>
#include <utils/hsearch.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/syscache.h>
/* for standard_conforming_strings */
#include <parser/gramparse.h>

#include <libpq-fe.h>

#if PG_VERSION_NUM < 80300

/*
 * Row version check for 8.2
 */
typedef struct RowStamp {
	TransactionId	xmin;
	CommandId		cmin;
} RowStamp;

static inline void plproxy_set_stamp(RowStamp *stamp, HeapTuple tup)
{
	stamp->xmin = HeapTupleHeaderGetXmin(tup->t_data);
	stamp->cmin = HeapTupleHeaderGetCmin(tup->t_data);
}

static inline bool plproxy_check_stamp(RowStamp *stamp, HeapTuple tup)
{
	return stamp->xmin == HeapTupleHeaderGetXmin(tup->t_data)
		&& stamp->cmin == HeapTupleHeaderGetCmin(tup->t_data);
}

#else /* ver >= 8.3 */

/*
 * Row version check for PG >= 8.3
 */
typedef struct RowStamp {
	TransactionId		xmin;
	ItemPointerData		tid;
} RowStamp;

static inline void plproxy_set_stamp(RowStamp *stamp, HeapTuple tup)
{
	stamp->xmin = HeapTupleHeaderGetXmin(tup->t_data);
	stamp->tid = procTup->t_self;
}

static inline bool plproxy_check_stamp(RowStamp *stamp, HeapTuple tup)
{
	return stamp->xmin == HeapTupleHeaderGetXmin(tup->t_data)
		&& ItemPointerEquals(&stamp->tid, &tup->t_self);
}
#endif

/*
 * Maintenece period in seconds.  Connnections will be freed
 * from stale results, and checked for lifetime.
 */
#define PLPROXY_MAINT_PERIOD		(2*60)

/*
 * Check connections that are idle more than this many seconds.
 * Undefine to disable.
 */
#define PLPROXY_IDLE_CONN_CHECK		10

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
	int			statement_timeout;		/* Do remotely: SET statement_timeout */
	int			disable_binary;			/* Avoid binary I/O */
} ProxyConfig;

/* Single database connection */
typedef struct
{
	const char *connstr;		/* Connection string for libpq */

	/* state */
	PGconn	   *db;				/* libpq connection handle */
	PGresult   *res;			/* last resultset */
	int			pos;			/* Current position inside res */
	ConnState	state;			/* Connection state */
	time_t		connect_time;	/* When connection was started */
	time_t		query_time;		/* When last query was sent */
	unsigned	run_on:1;		/* True it this connection should be used */
	unsigned	same_ver:1;		/* True if dest backend has same X.Y ver */
} ProxyConnection;

/* Info about one cluster */
typedef struct ProxyCluster
{
	struct ProxyCluster *next;	/* Pointer for building singly-linked list */

	const char *name;			/* Cluster name */
	int			version;		/* Cluster version */
	ProxyConfig config;			/* Cluster config */

	int			part_count;		/* Number of partitions - power of 2 */
	int			part_mask;		/* Mask to use to get part number from hash */
	ProxyConnection **part_map; /* Pointers to conn_list */

	int			conn_count;		/* Number of actual database connections */
	ProxyConnection *conn_list; /* List of actual database connections */

	int			ret_cur_conn;	/* Result walking: index of current conn */
	int			ret_cur_pos;	/* Result walking: index of current row */
	int			ret_total;		/* Result walking: total rows left */
} ProxyCluster;

/**
 * Type info cache.
 *
 * As the decision to send/receive binary may
 * change in runtime, both text and binary
 * function calls must be cached.
 */
typedef struct ProxyType
{
	Oid			type_oid;		/* Oid of the type */
	char	   *name;			/* Name of the type */

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

	Oid			io_param;		/* Extra arg for input_func */
	unsigned	for_send:1;		/* True if for outputting */
	unsigned	has_send:1;		/* Has binary output */
	unsigned	has_recv:1;		/* Has binary input */
	unsigned	by_value:1;		/* False if Datum is a pointer to data */
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
	char	  **name_list;		/* Column names */
	unsigned	use_binary:1;	/* True if all columns support binary recv */
} ProxyComposite;

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

/*
 * Complete info about compiled function.
 *
 * Note: only IN and INOUT arguments are cached here.
 */
typedef struct ProxyFunction
{
	const char *name;			/* Fully-quelified function name */
	Oid			oid;			/* Function OID */
	MemoryContext ctx;			/* Where runtime allocations should happen */

	RowStamp	stamp;			/* for pg_proc cache validation */

	int			arg_count;		/* Argument count of proxy function */
	ProxyType **arg_types;		/* Info about arguments */
	char	  **arg_names;		/* Argument names, may contain NULLs */

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

	/*
	 * Maps result field num to libpq column num.
	 * It is filled for each result.  NULL when scalar result.
	 */
	int		   *result_map;
} ProxyFunction;

/* main.c */
Datum		plproxy_call_handler(PG_FUNCTION_ARGS);
void		plproxy_error(ProxyFunction *func, const char *fmt,...);

/* function.c */
void		plproxy_function_cache_init(void);
void	   *plproxy_func_alloc(ProxyFunction *func, int size);
char	   *plproxy_func_strdup(ProxyFunction *func, const char *s);
ProxyFunction *plproxy_compile(FunctionCallInfo fcinfo, bool validate);

/* execute.c */
void		plproxy_exec(ProxyFunction *func, FunctionCallInfo fcinfo);
void		plproxy_clean_results(ProxyCluster *cluster);

/* scanner.c */
int			plproxy_yyget_lineno(void);
int			plproxy_yylex_destroy(void);
int			plproxy_yylex(void);
void		plproxy_scanner_sqlmode(bool val);

/* parser.y */
void		plproxy_run_parser(ProxyFunction *func, const char *body, int len);
void		plproxy_yyerror(const char *fmt,...);

/* type.c */
ProxyComposite *plproxy_composite_info(ProxyFunction *func, TupleDesc tupdesc);
ProxyType  *plproxy_find_type_info(ProxyFunction *func, Oid oid, bool for_send);
char	   *plproxy_send_type(ProxyType *type, Datum val, bool allow_bin, int *len, int *fmt);
Datum		plproxy_recv_type(ProxyType *type, char *str, int len, bool bin);
HeapTuple	plproxy_recv_composite(ProxyComposite *meta, char **values, int *lengths, int *fmts);

/* cluster.c */
void		plproxy_cluster_cache_init(void);
ProxyCluster *plproxy_find_cluster(ProxyFunction *func, FunctionCallInfo fcinfo);
void		plproxy_cluster_maint(struct timeval * now);

/* result.c */
Datum		plproxy_result(ProxyFunction *func, FunctionCallInfo fcinfo);

/* query.c */
QueryBuffer *plproxy_query_start(ProxyFunction *func, bool add_types);
bool		plproxy_query_add_const(QueryBuffer *q, const char *data);
bool		plproxy_query_add_ident(QueryBuffer *q, const char *ident);
ProxyQuery *plproxy_query_finish(QueryBuffer *q);
ProxyQuery *plproxy_standard_query(ProxyFunction *func, bool add_types);
void		plproxy_query_prepare(ProxyFunction *func, FunctionCallInfo fcinfo, ProxyQuery *q);
void		plproxy_query_exec(ProxyFunction *func, FunctionCallInfo fcinfo, ProxyQuery *q);
void		plproxy_query_freeplan(ProxyQuery *q);

#endif
