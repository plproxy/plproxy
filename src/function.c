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
 * Function compilation and caching.
 *
 * Functions here are called with CurrentMemoryContext == SPP Proc context.
 * They switch to per-function context only during allocations.
 */

#include "plproxy.h"


/*
 * Function cache entry.
 *
 * As PL/Proxy does not do trigger functions,
 * its enough to index just on OID.
 *
 * This structure is kept in HTAB's context.
 */
typedef struct
{
	/* Key value.	Must be at the start */
	Oid			oid;
	/* Pointer to function data */
	ProxyFunction *function;
}	HashEntry;

/* Function cache */
static HTAB *fn_cache = NULL;

/*
 * During compilation function is linked here.
 *
 * This avoids memleaks when throwing errors.
 */
static ProxyFunction *partial_func = NULL;



/* Allocate memory in the function's context */
void *
plproxy_func_alloc(ProxyFunction *func, int size)
{
	return MemoryContextAlloc(func->ctx, size);
}

/* Allocate string in the function's context */
char *
plproxy_func_strdup(ProxyFunction *func, const char *s)
{
	int			len = strlen(s) + 1;
	char	   *res = plproxy_func_alloc(func, len);

	memcpy(res, s, len);
	return res;
}

/* Find the index of a named parameter, -1 if not found */
int
plproxy_get_parameter_index(ProxyFunction *func, const char *ident)
{
	int		i;

	if (ident[0] == '$')
	{
		/* Probably a $# parameter reference */
		i = atoi(ident + 1) - 1;
		if (i >= 0 && i < func->arg_count)
			return i;
	}
	else if (func->arg_names)
	{
		/* Named parameter, go through the argument names */
		for (i = 0; i < func->arg_count; i++)
		{
			if (!func->arg_names[i])
				continue;
			if (pg_strcasecmp(ident, func->arg_names[i]) == 0)
				return i;
		}
	}

	return -1;
}

/* Add a new split argument by position */
static void
plproxy_split_add_arg(ProxyFunction *func, int argindex)
{
	if (!func->split_args)
	{
		size_t alloc_size = sizeof(*func->split_args) * func->arg_count;

		func->split_args = plproxy_func_alloc(func, alloc_size);
		MemSet(func->split_args, 0, alloc_size);
	}

	func->split_args[argindex] = true;
}

/* Add a new split argument by argument name */
bool
plproxy_split_add_ident(ProxyFunction *func, const char *ident)
{
	int		argindex;

	if ((argindex = plproxy_get_parameter_index(func, ident)) < 0)
		return false;

	/* Already split? */
	if (IS_SPLIT_ARG(func, argindex))
		plproxy_error(func, "SPLIT parameter specified more than once: %s", ident);

	/* Is it an array? */
	if (!func->arg_types[argindex]->is_array)
		plproxy_error(func, "SPLIT parameter is not an array: %s", ident);

	plproxy_split_add_arg(func, argindex);

	return true;
}

/* Tag all array arguments for splitting */
void
plproxy_split_all_arrays(ProxyFunction *func)
{
	int		i;

	for (i = 0; i < func->arg_count; i++) {
		if (func->arg_types[i]->is_array)
			plproxy_split_add_arg(func, i);
	}
}

/* Initialize PL/Proxy function cache */
void
plproxy_function_cache_init(void)
{
	HASHCTL		ctl;
	int			flags;
	int			max_funcs = 128;

	/* don't allow multiple initializations */
	Assert(fn_cache == NULL);

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(HashEntry);
	ctl.hash = oid_hash;
	flags = HASH_ELEM | HASH_FUNCTION;
	fn_cache = hash_create("PL/Proxy function cache", max_funcs, &ctl, flags);
}


/* Search for function in cache */
static ProxyFunction *
fn_cache_lookup(Oid fn_oid)
{
	HashEntry  *hentry;

	hentry = hash_search(fn_cache, &fn_oid, HASH_FIND, NULL);
	if (hentry)
		return hentry->function;
	return NULL;
}


/* Insert function into cache */
static void
fn_cache_insert(ProxyFunction *func)
{
	HashEntry  *hentry;
	bool		found;

	hentry = hash_search(fn_cache, &func->oid, HASH_ENTER, &found);
	Assert(found == false);

	hentry->function = func;
}


/* Delete function from cache */
static void
fn_cache_delete(ProxyFunction *func)
{
	HashEntry  *hentry;

	hentry = hash_search(fn_cache, &func->oid, HASH_REMOVE, NULL);
	Assert(hentry != NULL);
}

/* check if function returns untyped RECORD which needs the AS clause */
static bool
fn_returns_dynamic_record(HeapTuple proc_tuple)
{
	Form_pg_proc proc_struct;
	proc_struct = (Form_pg_proc) GETSTRUCT(proc_tuple);
	if (proc_struct->prorettype == RECORDOID
		&& (heap_attisnull(proc_tuple, Anum_pg_proc_proargmodes)
		    || heap_attisnull(proc_tuple, Anum_pg_proc_proargnames)))
		return true;
	return false;
}

/*
 * Allocate storage for function.
 *
 * Each functions has its own MemoryContext,
 * where everything is allocated.
 */
static ProxyFunction *
fn_new(FunctionCallInfo fcinfo, HeapTuple proc_tuple)
{
	ProxyFunction *f;
	MemoryContext f_ctx,
				old_ctx;

	f_ctx = AllocSetContextCreate(TopMemoryContext,
								  "PL/Proxy function context",
								  ALLOCSET_SMALL_MINSIZE,
								  ALLOCSET_SMALL_INITSIZE,
								  ALLOCSET_SMALL_MAXSIZE);

	old_ctx = MemoryContextSwitchTo(f_ctx);

	f = palloc0(sizeof(*f));
	f->ctx = f_ctx;
	f->oid = fcinfo->flinfo->fn_oid;
	plproxy_set_stamp(&f->stamp, proc_tuple);

	if (fn_returns_dynamic_record(proc_tuple))
		f->dynamic_record = 1;

	MemoryContextSwitchTo(old_ctx);

	return f;
}


/*
 * Delete function and release all associated storage
 *
 * Function is also deleted from cache.
 */
static void
fn_delete(ProxyFunction *func, bool in_cache)
{
	if (in_cache)
		fn_cache_delete(func);

	/* free cached plans */
	plproxy_query_freeplan(func->hash_sql);
	plproxy_query_freeplan(func->cluster_sql);
	plproxy_query_freeplan(func->connect_sql);

	/* release function storage */
	MemoryContextDelete(func->ctx);
}

/*
 * Construct fully-qualified name for function.
 */
static void
fn_set_name(ProxyFunction *func, HeapTuple proc_tuple)
{
	/* 2 names, size can double, "" + . + "" + NUL */
	char		namebuf[NAMEDATALEN * 4 + 2 + 1 + 2 + 1];
	Form_pg_proc proc_struct;
	Form_pg_namespace ns_struct;
	HeapTuple	ns_tup;
	Oid			nsoid;

	proc_struct = (Form_pg_proc) GETSTRUCT(proc_tuple);
	nsoid = proc_struct->pronamespace;

	ns_tup = SearchSysCache(NAMESPACEOID,
							ObjectIdGetDatum(nsoid), 0, 0, 0);
	if (!HeapTupleIsValid(ns_tup))
		plproxy_error(func, "Cannot find namespace %u", nsoid);
	ns_struct = (Form_pg_namespace) GETSTRUCT(ns_tup);

	snprintf(namebuf, sizeof(namebuf), "%s.%s",
			quote_identifier(NameStr(ns_struct->nspname)),
			quote_identifier(NameStr(proc_struct->proname)));
	func->name = plproxy_func_strdup(func, namebuf);

	ReleaseSysCache(ns_tup);
}

/*
 * Parse source.
 *
 * It just fetches source and calls actual parser.
 */
static void
fn_parse(ProxyFunction *func, HeapTuple proc_tuple)
{
	bool		isnull;
	Datum		src_raw, src_detoast;
	char		*data;
	int			size;

	src_raw = SysCacheGetAttr(PROCOID, proc_tuple, Anum_pg_proc_prosrc, &isnull);
	if (isnull)
		plproxy_error(func, "procedure source datum is null");

	src_detoast = PointerGetDatum(PG_DETOAST_DATUM_PACKED(src_raw));
	data = VARDATA_ANY(src_detoast);
	size = VARSIZE_ANY_EXHDR(src_detoast);

	plproxy_run_parser(func, data, size);

	if (src_raw != src_detoast)
		pfree(DatumGetPointer(src_detoast));
}

/*
 * Get info about own arguments.
 */
static void
fn_get_arguments(ProxyFunction *func,
				 FunctionCallInfo fcinfo,
				 HeapTuple proc_tuple)
{
	Oid		   *types;
	char	  **names,
			   *modes;
	int			i,
				pos,
				total;
	ProxyType  *type;

	total = get_func_arg_info(proc_tuple, &types, &names, &modes);

	func->arg_types = plproxy_func_alloc(func, sizeof(ProxyType *) * total);
	func->arg_names = plproxy_func_alloc(func, sizeof(char *) * total);
	func->arg_count = 0;

	for (i = 0; i < total; i++)
	{
		char mode = modes ? modes[i] : PROARGMODE_IN;
		switch (mode) {
		case PROARGMODE_IN:
		case PROARGMODE_INOUT:
			type = plproxy_find_type_info(func, types[i], 1);
			pos = func->arg_count++;
			func->arg_types[pos] = type;
			if (names && names[i])
				func->arg_names[pos] = plproxy_func_strdup(func, names[i]);
			else
				func->arg_names[pos] = NULL;
			break;
		case PROARGMODE_VARIADIC:
			elog(ERROR, "PL/Proxy does not support variadic args");
			break;
		case PROARGMODE_OUT:
		case PROARGMODE_TABLE:
			/* output args, ignore */
			break;
		default:
			elog(ERROR, "PL/Proxy: unknown value in proargmodes: %c", mode);
			break;
		}
	}
}

/*
 * Get info about return type.
 *
 * Fills one of ret_scalar or ret_composite.
 */
static void
fn_get_return_type(ProxyFunction *func,
				   FunctionCallInfo fcinfo,
				   HeapTuple proc_tuple)
{
	Oid			ret_oid;
	TupleDesc	ret_tup;
	TypeFuncClass rtc;
	MemoryContext old_ctx;
	int			natts;


	/*
	 * get_call_result_type() will return newly allocated tuple,
	 * except in case of untyped RECORD functions.
	 */
	old_ctx = MemoryContextSwitchTo(func->ctx);
	rtc = get_call_result_type(fcinfo, &ret_oid, &ret_tup);
	if (func->dynamic_record && ret_tup)
		ret_tup = CreateTupleDescCopy(ret_tup);
	MemoryContextSwitchTo(old_ctx);

	switch (rtc)
	{
		case TYPEFUNC_COMPOSITE:
			func->ret_composite = plproxy_composite_info(func, ret_tup);
			natts = func->ret_composite->tupdesc->natts;
			func->result_map = plproxy_func_alloc(func, natts * sizeof(int));
			break;
		case TYPEFUNC_SCALAR:
			func->ret_scalar = plproxy_find_type_info(func, ret_oid, 0);
			func->result_map = NULL;
			break;
		case TYPEFUNC_RECORD:
		case TYPEFUNC_OTHER:
			/* fixme: void type here? */
			plproxy_error(func, "unsupported type");
			break;
	}
}

/*
 * Check if cached ->ret_composite is valid, refresh if needed.
 */
static void
fn_refresh_record(FunctionCallInfo fcinfo, 
				  ProxyFunction *func,
				  HeapTuple proc_tuple) 
{

	TypeFuncClass rtc;
	TupleDesc tuple_current, tuple_cached;
	MemoryContext old_ctx;
	Oid tuple_oid;
	int natts;

	/*
	 * Compare cached tuple to current one.
	 */
	tuple_cached = func->ret_composite->tupdesc;
	rtc = get_call_result_type(fcinfo, &tuple_oid, &tuple_current);
	Assert(rtc == TYPEFUNC_COMPOSITE);
	if (equalTupleDescs(tuple_current, tuple_cached))
		return;

	/* move to function context */
	old_ctx = MemoryContextSwitchTo(func->ctx);
	tuple_current = CreateTupleDescCopy(tuple_current);
	MemoryContextSwitchTo(old_ctx);

	/* release old data */
	plproxy_free_composite(func->ret_composite);
	pfree(func->result_map);
	pfree(func->remote_sql);

	/* construct new data */
	func->ret_composite = plproxy_composite_info(func, tuple_current);
	natts = func->ret_composite->tupdesc->natts;
	func->result_map = plproxy_func_alloc(func, natts * sizeof(int));
	func->remote_sql = plproxy_standard_query(func, true);
}

/* Show part of compilation -- get source and parse */
static ProxyFunction *
fn_compile(FunctionCallInfo fcinfo,
		   HeapTuple proc_tuple,
		   bool validate)
{
	ProxyFunction *f;
	Form_pg_proc proc_struct;

	proc_struct = (Form_pg_proc) GETSTRUCT(proc_tuple);
	if (proc_struct->provolatile != PROVOLATILE_VOLATILE)
		elog(ERROR, "PL/Proxy functions must be volatile");

	f = fn_new(fcinfo, proc_tuple);

	/* keep reference in case of error half-way */
	partial_func = f;

	/* info from system tables */
	fn_set_name(f, proc_tuple);
	fn_get_return_type(f, fcinfo, proc_tuple);
	fn_get_arguments(f, fcinfo, proc_tuple);

	/* parse body */
	fn_parse(f, proc_tuple);

	if (f->dynamic_record && f->remote_sql)
		plproxy_error(f, "SELECT statement not allowed for dynamic RECORD functions");

	/* create SELECT stmt if not specified */
	if (f->remote_sql == NULL)
		f->remote_sql = plproxy_standard_query(f, true);

	/* prepare local queries */
	if (f->cluster_sql)
		plproxy_query_prepare(f, fcinfo, f->cluster_sql, false);
	if (f->hash_sql)
		plproxy_query_prepare(f, fcinfo, f->hash_sql, true);
	if (f->connect_sql)
		plproxy_query_prepare(f, fcinfo, f->connect_sql, false);

	/* sanity check */
	if (f->run_type == R_ALL && !fcinfo->flinfo->fn_retset)
		plproxy_error(f, "RUN ON ALL requires set-returning function");

	return f;
}

/*
 * Compile and cache PL/Proxy function.
 */
ProxyFunction *
plproxy_compile(FunctionCallInfo fcinfo, bool validate)
{
	ProxyFunction *f;
	HeapTuple	proc_tuple;
	Oid			oid;

	/* clean interrupted compile */
	if (partial_func)
	{
		fn_delete(partial_func, false);
		partial_func = NULL;
	}

	/* get current fn oid */
	oid = fcinfo->flinfo->fn_oid;

	/* lookup the pg_proc tuple */
	proc_tuple = SearchSysCache(PROCOID, ObjectIdGetDatum(oid), 0, 0, 0);
	if (!HeapTupleIsValid(proc_tuple))
		elog(ERROR, "cache lookup failed for function %u", oid);

	/* fn_extra not used, do lookup */
	f = fn_cache_lookup(oid);

	/* if cached, is it still valid? */
	if (f && !plproxy_check_stamp(&f->stamp, proc_tuple))
	{
		fn_delete(f, true);
		f = NULL;
	}

	if (!f)
	{
		f = fn_compile(fcinfo, proc_tuple, validate);

		fn_cache_insert(f);

		/* now its safe to drop reference */
		partial_func = NULL;
	}
	else if (f->dynamic_record)
	{
		/* in case of untyped RECORD, check if cached type is valid */
		fn_refresh_record(fcinfo, f, proc_tuple);
	}
	else if (f->ret_composite)
	{
		if (!plproxy_composite_valid(f->ret_composite))
			fn_refresh_record(fcinfo, f, proc_tuple);
	}

	ReleaseSysCache(proc_tuple);

	return f;
}
