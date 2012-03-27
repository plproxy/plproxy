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
 * External interface for PostgreSQL core.
 *
 * List of memory contexts that are touched by this code:
 *
 * - Query context that is active when plproxy_call_handler is called.
 *	 Function results should be allocated from here.
 *
 * - SPI Proc context that activates in SPI_connect() and is freed
 *	 in SPI_finish().  This is used for compile-time short-term storage.
 *
 * - HTAB has its own memory context.
 *
 * - ProxyFunction->ctx for long-term allocations for functions.
 *
 * - cluster_mem where info about clusters is stored.
 *
 * - SPI_saveplan() stores plan info in separate context,
 *	 so it must be freed explicitly.
 *
 * - libpq uses malloc() so it must be freed explicitly
 *
 * Because SPI functions do not honour CurrentMemoryContext
 * and code should not have assumptions whether core
 * functions do allocations or not, the per-function and
 * cluster MemoryContext is switched on only when doing actual
 * allocations.  Otherwise the default context is kept.
 */

#include "plproxy.h"

#include <sys/time.h>

#ifndef PG_MODULE_MAGIC
#error PL/Proxy requires 8.2
#else
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1(plproxy_call_handler);

/*
 * Centralised error reporting.
 *
 * Also frees any pending results.
 */
void
plproxy_error(ProxyFunction *func, const char *fmt,...)
{
	char		msg[1024];
	va_list		ap;

	va_start(ap, fmt);
	vsnprintf(msg, sizeof(msg), fmt, ap);
	va_end(ap);

	plproxy_clean_results(func->cur_cluster);

	elog(ERROR, "PL/Proxy function %s(%d): %s",
		 func->name, func->arg_count, msg);
}

/*
 * Pass remote error/notice/warning through.
 */
void
plproxy_remote_error(ProxyFunction *func, ProxyConnection *conn, const PGresult *res, bool iserr)
{
	const char *ss = PQresultErrorField(res, PG_DIAG_SQLSTATE);
	const char *sev = PQresultErrorField(res, PG_DIAG_SEVERITY);
	const char *msg = PQresultErrorField(res, PG_DIAG_MESSAGE_PRIMARY);
	const char *det = PQresultErrorField(res, PG_DIAG_MESSAGE_DETAIL);
	const char *hint = PQresultErrorField(res, PG_DIAG_MESSAGE_HINT);
	const char *spos = PQresultErrorField(res, PG_DIAG_STATEMENT_POSITION);
	const char *ipos = PQresultErrorField(res, PG_DIAG_INTERNAL_POSITION);
	const char *iquery = PQresultErrorField(res, PG_DIAG_INTERNAL_QUERY);
	const char *ctx = PQresultErrorField(res, PG_DIAG_CONTEXT);
	int elevel;

	/* libpq errors may not have sqlstate */
	if (!ss)
		ss = "XX000";

	if (iserr)
		/* must ignore remote level, as it may be FATAL/PANIC */
		elevel = ERROR;
	else
		/* cannot look at sev here, as it may be localized */
		elevel = !strncmp(ss, "00", 2) ? NOTICE : WARNING;

	ereport(elevel, (
		errcode(MAKE_SQLSTATE(ss[0], ss[1], ss[2], ss[3], ss[4])),
		errmsg("%s(%d): [%s] REMOTE %s: %s", func->name, func->arg_count, PQdb(conn->cur->db), sev, msg),
		det ? errdetail("Remote detail: %s", det) : 0,
		hint ? errhint("Remote hint: %s", hint) : 0,
		spos ? errposition(atoi(spos)) : 0,
		ipos ? internalerrposition(atoi(ipos)) : 0,
		iquery ? internalerrquery(iquery) : 0,
		ctx ? errcontext("Remote context: %s", ctx) : 0));
}

/*
 * Library load-time initialization.
 * Do the initialization when SPI is active to simplify the code.
 */
static bool initialized = false;

static void
plproxy_startup_init(void)
{
	if (initialized)
		return;

	plproxy_function_cache_init();
	plproxy_cluster_cache_init();
	plproxy_syscache_callback_init();

	initialized = true;
}

/*
 * Regular maintenance over all clusters.
 */
static void
run_maint(void)
{
	static struct timeval last = {0, 0};
	struct timeval now;

	if (!initialized)
		return;

	gettimeofday(&now, NULL);
	if (now.tv_sec - last.tv_sec < 2 * 60)
		return;
	last = now;

	plproxy_cluster_maint(&now);
}

/*
 * Do compilation and execution under SPI.
 *
 * Result conversion will be done without SPI.
 */
static ProxyFunction *
compile_and_execute(FunctionCallInfo fcinfo)
{
	int			err;
	ProxyFunction *func;
	ProxyCluster *cluster;

	/* prepare SPI */
	err = SPI_connect();
	if (err != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect: %s", SPI_result_code_string(err));

	/* do the initialization also under SPI */
	plproxy_startup_init();

	/* compile code */
	func = plproxy_compile(fcinfo, false);

	/* get actual cluster to run on */
	cluster = plproxy_find_cluster(func, fcinfo);

	/* Don't allow nested calls on the same cluster */
	if (cluster->busy)
		plproxy_error(func, "Nested PL/Proxy calls to the same cluster are not supported.");

	/* fetch PGresults */
	func->cur_cluster = cluster;
	plproxy_exec(func, fcinfo);

	/* done with SPI */
	err = SPI_finish();
	if (err != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish: %s", SPI_result_code_string(err));

	return func;
}

/*
 * Logic for set-returning functions.
 *
 * Currently it uses the simplest, return
 * one value/tuple per call mechanism.
 */
static Datum
handle_ret_set(FunctionCallInfo fcinfo)
{
	ProxyFunction *func;
	FuncCallContext *ret_ctx;

	if (SRF_IS_FIRSTCALL())
	{
		func = compile_and_execute(fcinfo);
		ret_ctx = SRF_FIRSTCALL_INIT();
		ret_ctx->user_fctx = func;
	}

	ret_ctx = SRF_PERCALL_SETUP();
	func = ret_ctx->user_fctx;

	if (func->cur_cluster->ret_total > 0)
	{
		SRF_RETURN_NEXT(ret_ctx, plproxy_result(func, fcinfo));
	}
	else
	{
		plproxy_clean_results(func->cur_cluster);
		SRF_RETURN_DONE(ret_ctx);
	}
}

/*
 * The PostgreSQL function & trigger manager calls this function
 * for execution of PL/Proxy procedures.
 *
 * Main entry point for rest of the code.
 */
Datum
plproxy_call_handler(PG_FUNCTION_ARGS)
{
	ProxyFunction *func;
	Datum		ret;

	if (CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "PL/Proxy procedures can't be used as triggers");

	/* clean old results */
	if (!fcinfo->flinfo->fn_retset || SRF_IS_FIRSTCALL())
		run_maint();

	if (fcinfo->flinfo->fn_retset)
	{
		ret = handle_ret_set(fcinfo);
	}
	else
	{
		func = compile_and_execute(fcinfo);
		if (func->cur_cluster->ret_total != 1)
			plproxy_error(func, "Non-SETOF function requires 1 row from remote query, got %d",
						  func->cur_cluster->ret_total);
		ret = plproxy_result(func, fcinfo);
		plproxy_clean_results(func->cur_cluster);
	}
	return ret;
}
