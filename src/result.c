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
 * Conversion from PGresult to Datum.
 *
 * Functions here are called with CurrentMemoryContext == query context
 * so that palloc()-ed memory stays valid after return to postgres.
 */

#include "plproxy.h"

static bool
name_matches(ProxyFunction *func, const char *aname, PGresult *res, int col)
{
	const char *fname = PQfname(res, col);

	if (fname == NULL)
		plproxy_error(func, "Unnamed result column %d", col + 1);
	if (strcmp(aname, fname) == 0)
		return true;
	return false;
}

/* fill func->result_map */
static void
map_results(ProxyFunction *func, PGresult *res)
{
	int			i,  /* non-dropped column index */
				xi, /* tupdesc index */
				j,  /* result column index */
				natts,
				nfields = PQnfields(res);
	Form_pg_attribute a;
	const char *aname;

	if (func->ret_scalar)
	{
		if (nfields != 1)
			plproxy_error(func,
						  "single field function but got record");
		return;
	}

	natts = func->ret_composite->tupdesc->natts;
	if (nfields < func->ret_composite->nfields)
		plproxy_error(func, "Got too few fields from remote end");
	if (nfields > func->ret_composite->nfields)
		plproxy_error(func, "Got too many fields from remote end");

	for (i = -1, xi = 0; xi < natts; xi++)
	{
		/* ->name_list has quoted names, take unquoted from ->tupdesc */
		a = func->ret_composite->tupdesc->attrs[xi];

		func->result_map[xi] = -1;

		if (a->attisdropped)
			continue;
		i++;

		aname = NameStr(a->attname);
		if (name_matches(func, aname, res, i))
			/* fast case: 1:1 mapping */
			func->result_map[xi] = i;
		else
		{
			/* slow case: messed up ordering */
			for (j = 0; j < nfields; j++)
			{
				/* already tried this one */
				if (j == i)
					continue;

				/*
				 * fixme: somehow remember the ones that are already mapped?
				 */
				if (name_matches(func, aname, res, j))
				{
					func->result_map[xi] = j;
					break;
				}
			}
		}
		if (func->result_map[xi] < 0)
			plproxy_error(func,
						  "Field %s does not exists in result", aname);
	}
}

/* Return connection where are unreturned rows */
static ProxyConnection *
walk_results(ProxyFunction *func, ProxyCluster *cluster)
{
	ProxyConnection *conn;

	for (; cluster->ret_cur_conn < cluster->active_count;
		 cluster->ret_cur_conn++)
	{
		conn = cluster->active_list[cluster->ret_cur_conn];
		if (conn->res == NULL)
			continue;
		if (conn->pos == PQntuples(conn->res))
			continue;

		/* first time on this connection? */
		if (conn->pos == 0)
			map_results(func, conn->res);

		return conn;
	}

	plproxy_error(func, "bug: no result");
	return NULL;
}

/* Return a tuple */
static Datum
return_composite(ProxyFunction *func, ProxyConnection *conn, FunctionCallInfo fcinfo)
{
	int			i,
				col;
	char	  **values;
	int		   *fmts;
	int		   *lengths;
	HeapTuple	tup;
	ProxyComposite *meta = func->ret_composite;

	values = palloc(meta->tupdesc->natts * sizeof(char *));
	fmts = palloc(meta->tupdesc->natts * sizeof(int));
	lengths = palloc(meta->tupdesc->natts * sizeof(int));

	for (i = 0; i < meta->tupdesc->natts; i++)
	{
		col = func->result_map[i];
		if (col < 0 || PQgetisnull(conn->res, conn->pos, col))
		{
			values[i] = NULL;
			lengths[i] = 0;
			fmts[i] = 0;
		}
		else
		{
			values[i] = PQgetvalue(conn->res, conn->pos, col);
			lengths[i] = PQgetlength(conn->res, conn->pos, col);
			fmts[i] = PQfformat(conn->res, col);
		}
	}
	tup = plproxy_recv_composite(meta, values, lengths, fmts);

	pfree(lengths);
	pfree(fmts);
	pfree(values);

	return HeapTupleGetDatum(tup);
}

/* Return scalar value */
static Datum
return_scalar(ProxyFunction *func, ProxyConnection *conn, FunctionCallInfo fcinfo)
{
	Datum		dat;
	char	   *val;
	PGresult   *res = conn->res;
	int			row = conn->pos;

	if (func->ret_scalar->type_oid == VOIDOID)
	{
		dat = (Datum) NULL;
	}
	else if (PQgetisnull(res, row, 0))
	{
		fcinfo->isnull = true;
		dat = (Datum) NULL;
	}
	else
	{
		val = PQgetvalue(res, row, 0);
		if (val == NULL)
			plproxy_error(func, "unexcpected NULL");
		dat = plproxy_recv_type(func->ret_scalar, val,
								PQgetlength(res, row, 0),
								PQfformat(res, 0));
	}
	return dat;
}

/* Return next result Datum */
Datum
plproxy_result(ProxyFunction *func, FunctionCallInfo fcinfo)
{
	Datum		dat;
	ProxyCluster *cluster = func->cur_cluster;
	ProxyConnection *conn;

	conn = walk_results(func, cluster);

	if (func->ret_composite)
		dat = return_composite(func, conn, fcinfo);
	else
		dat = return_scalar(func, conn, fcinfo);

	cluster->ret_total--;
	conn->pos++;

	return dat;
}
