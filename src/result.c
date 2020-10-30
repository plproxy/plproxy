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

/* fill conn->result_map */
static void
map_results(ProxyFunction *func, ProxyConnection *conn, PGresult *res)
{
	int			i,  /* non-dropped column index */
				xi, /* tupdesc index */
				j,  /* result column index */
				natts,
				nfields = PQnfields(res);
	Form_pg_attribute a;
	const char *aname;

	if (conn->result_map)
		pfree(conn->result_map);
	conn->result_map = NULL;

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

	conn->result_map = plproxy_allocate_memory(natts * sizeof(int));

	for (i = -1, xi = 0; xi < natts; xi++)
	{
		/* ->name_list has quoted names, take unquoted from ->tupdesc */
		a = TupleDescAttr(func->ret_composite->tupdesc, xi);

		conn->result_map[xi] = -1;

		if (a->attisdropped)
			continue;
		i++;

		aname = NameStr(a->attname);
		if (name_matches(func, aname, res, i))
			/* fast case: 1:1 mapping */
			conn->result_map[xi] = i;
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
					conn->result_map[xi] = j;
					break;
				}
			}
		}
		if (conn->result_map[xi] < 0)
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
			map_results(func, conn, conn->res);

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
		col = conn->result_map[i];
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

/*
 * Build a HeapTuple from a single-row query result.
 */
HeapTuple
plproxy_tuple_from_result(PGresult *res, TupleDesc tupdesc, ProxyFunction *func, ProxyConnection *conn)
{
	int	nfields = PQnfields(res);
	HeapTuple tuple;
	MemoryContext old_ctx;

	/*
	 * check result and tuple descriptor have the same number of columns
	 */
	if (PQnfields(res) != tupdesc->natts)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("remote query result rowtype does not match "
						"the specified FROM clause rowtype")));

	/* get the result column mapping when the first row is processed */
	if (conn->pos == 0)
		map_results(func, conn, res);
	++conn->pos;

	/* switch to temporary memory context */
	old_ctx = MemoryContextSwitchTo(func->tuplectx);
	/* clear temporary memory context */
	MemoryContextReset(func->tuplectx);

	/*
	 * We can safely assume that the values are either all binary
	 * or all textual.
	 */
	if (nfields > 0 && PQfformat(res, 0) == 1)
	{
		/*
		 * This branch is currently dead code, since binary mode has been
		 * disabled in 4a8a66270b29f78c9d5c082852ca26902517a7e4
		 */
		Datum *values = (Datum *) palloc(nfields * sizeof(Datum));
		bool  *nulls = (bool *) palloc(nfields * sizeof(bool));
		int    i;

		/* result contains only binary data */
		for (i = 0; i < nfields; i++)
		{
			Datum d = 0;
			int typsize;
			int colpos = conn->result_map ? conn->result_map[i] : i;

			if (PQgetisnull(res, 0, colpos))
			{
				nulls[i] = true;
				continue;
			}

			nulls[i] = false;

			/* convert binary representation to Datum */
			if ((typsize = PQfsize(res, colpos)) == -1)
			{
				/* varlena */
				int dlen = PQgetlength(res, 0, colpos);
				struct varlena *v = palloc(dlen + VARHDRSZ);

				SET_VARSIZE(v, dlen + VARHDRSZ);
				memcpy(VARDATA(v), PQgetvalue(res, 0, colpos), dlen);

				d = PointerGetDatum(v);
			}
			else
			{
				union {
					int32 i4[2];
					int64 i8;
				} x;
				int32 s;

				/* must convert from network byte order to host byte order */
				switch(typsize)
				{
					case 1:
						d = Int8GetDatum(*(int32 *)(PQgetvalue(res, 0, colpos)));
						break;
					case 2:
						d = Int16GetDatum(ntohs(*(int32 *)(PQgetvalue(res, 0, colpos))));
						break;
					case 4:
						d = Int32GetDatum(ntohl(*(int32 *)(PQgetvalue(res, 0, colpos))));
						break;
					case 8:
						x.i8 = *(int64 *)(PQgetvalue(res, 0, colpos));
#ifndef WORDS_BIGENDIAN
						s = ntohl(x.i4[0]);
						x.i4[0] = ntohl(x.i4[1]);
						x.i4[1] = s;
#endif
						d = Int64GetDatum(x.i8);
						break;
				}
			}

			values[i] = d;
		}

		tuple = heap_form_tuple(tupdesc, values, nulls);
	}
	else
	{
		int				i;
		char		  **values;
		AttInMetadata  *attinmeta = TupleDescGetAttInMetadata(tupdesc);

		/* result contains only textual data */
		if (nfields > 0)
			values = (char **) palloc(nfields * sizeof(char *));
		else
			values = NULL;

		/* tuple consists of textual data */
		for (i = 0; i < nfields; i++)
		{
			int colpos = conn->result_map ? conn->result_map[i] : i;

			if (PQgetisnull(res, 0, colpos))
				values[i] = NULL;
			else
				values[i] = PQgetvalue(res, 0, colpos);
		}

		tuple = BuildTupleFromCStrings(attinmeta, values);
	}

	/* switch back to persistent memory context */
	(void)MemoryContextSwitchTo(old_ctx);

	return tuple;
}
