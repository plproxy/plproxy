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
 * Caches I/O info about scalar values.
 */

#include "plproxy.h"

/*
 * Checks if we can safely use binary.
 */
static bool usable_binary(Oid oid)
{
	switch (oid)
	{
		case BOOLOID:
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case TEXTOID:
		case BPCHAROID:
		case VARCHAROID:
		case FLOAT4OID:
		case FLOAT8OID:
		case NUMERICOID:
		case BYTEAOID:
			return true;

		/* integer vs. float issue */
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
		case DATEOID:
		case TIMEOID:

		/* interval binary fmt changed in 8.1 */
		case INTERVALOID:
		default:
			return false;
	}
}

/*
 * Collects info about fields of a composite type.
 *
 * Based on TupleDescGetAttInMetadata.
 */
ProxyComposite *
plproxy_composite_info(ProxyFunction *func, TupleDesc tupdesc)
{
	int			i,
				natts = tupdesc->natts;
	ProxyComposite *ret;
	MemoryContext old_ctx;
	Form_pg_attribute a;
	ProxyType  *type;
	const char *name;

	old_ctx = MemoryContextSwitchTo(func->ctx);

	ret = palloc(sizeof(*ret));
	ret->type_list = palloc(sizeof(ProxyType *) * natts);
	ret->name_list = palloc0(sizeof(char *) * natts);
	ret->tupdesc = BlessTupleDesc(tupdesc);
	ret->use_binary = 1;

	MemoryContextSwitchTo(old_ctx);

	for (i = 0; i < natts; i++)
	{
		a = tupdesc->attrs[i];
		if (a->attisdropped)
			plproxy_error(func, "dropped attrs not supported");

		name = quote_identifier(NameStr(a->attname));
		ret->name_list[i] = plproxy_func_strdup(func, name);

		type = plproxy_find_type_info(func, a->atttypid, 0);
		ret->type_list[i] = type;

		if (!type->has_recv)
			ret->use_binary = 0;
	}

	return ret;
}

/*
 * Build result tuplw from binary or CString values.
 *
 * Based on BuildTupleFromCStrings.
 */
HeapTuple
plproxy_recv_composite(ProxyComposite *meta, char **values, int *lengths, int *fmts)
{
	TupleDesc	tupdesc = meta->tupdesc;
	int			natts = tupdesc->natts;
	Datum	   *dvalues;
	char	   *nulls;
	int			i;
	HeapTuple	tuple;

	dvalues = (Datum *) palloc(natts * sizeof(Datum));
	nulls = (char *) palloc(natts * sizeof(char));

	/* Call the recv function for each attribute */
	for (i = 0; i < natts; i++)
	{
		if (tupdesc->attrs[i]->attisdropped)
			elog(ERROR, "dropped attrs not supported");

		dvalues[i] = plproxy_recv_type(meta->type_list[i],
									   values[i], lengths[i], fmts[i]);
		nulls[i] = (values[i] != NULL) ? ' ' : 'n';
	}

	/* Form a tuple */
	tuple = heap_formtuple(tupdesc, dvalues, nulls);

	/*
	 * Release locally palloc'd space.
	 */
	for (i = 0; i < natts; i++)
	{
		if (nulls[i] == 'n')
			continue;
		if (meta->type_list[i]->by_value)
			continue;
		pfree(DatumGetPointer(dvalues[i]));
	}
	pfree(dvalues);
	pfree(nulls);

	return tuple;
}

/* Find info about scalar type */
ProxyType *
plproxy_find_type_info(ProxyFunction *func, Oid oid, bool for_send)
{
	ProxyType  *type;
	HeapTuple	t_type,
				t_nsp;
	Form_pg_type s_type;
	Form_pg_namespace s_nsp;
	char		namebuf[NAMEDATALEN * 4 + 2 + 1 + 2 + 1];
	Oid			nsoid;

	/* fetch pg_type row */
	t_type = SearchSysCache(TYPEOID, ObjectIdGetDatum(oid), 0, 0, 0);
	if (!HeapTupleIsValid(t_type))
		plproxy_error(func, "cache lookup failed for type %u", oid);

	/* typname, typnamespace, PG_CATALOG_NAMESPACE, PG_PUBLIC_NAMESPACE */
	s_type = (Form_pg_type) GETSTRUCT(t_type);
	nsoid = s_type->typnamespace;

	if (nsoid != PG_CATALOG_NAMESPACE)
	{
		t_nsp = SearchSysCache(NAMESPACEOID, ObjectIdGetDatum(nsoid), 0, 0, 0);
		if (!HeapTupleIsValid(t_nsp))
			plproxy_error(func, "cache lookup failed for namespace %u", nsoid);
		s_nsp = (Form_pg_namespace) GETSTRUCT(t_nsp);
		snprintf(namebuf, sizeof(namebuf), "%s.%s",
				quote_identifier(NameStr(s_nsp->nspname)),
				quote_identifier(NameStr(s_type->typname)));
		ReleaseSysCache(t_nsp);
	}
	else
	{
		snprintf(namebuf, sizeof(namebuf), "%s", quote_identifier(NameStr(s_type->typname)));
	}

	/* sanity check */
	switch (s_type->typtype)
	{
		default:
		case 'p':
			if (oid != VOIDOID)
				plproxy_error(func, "unsupported pseudo type: %s (%u)",
							  namebuf, oid);
		case 'b':
		case 'c':
		case 'd':
			break;
	}

	/* allocate & fill structure */
	type = plproxy_func_alloc(func, sizeof(*type));
	memset(type, 0, sizeof(*type));

	type->type_oid = oid;
	type->io_param = getTypeIOParam(t_type);
	type->for_send = for_send;
	type->by_value = s_type->typbyval;
	type->name = plproxy_func_strdup(func, namebuf);

	/* decide what function is needed */
	if (for_send)
	{
		fmgr_info_cxt(s_type->typoutput, &type->io.out.output_func, func->ctx);
		if (OidIsValid(s_type->typsend) && usable_binary(oid))
		{
			fmgr_info_cxt(s_type->typsend, &type->io.out.send_func, func->ctx);
			type->has_send = 1;
		}
	}
	else
	{
		fmgr_info_cxt(s_type->typinput, &type->io.in.input_func, func->ctx);
		if (OidIsValid(s_type->typreceive) && usable_binary(oid))
		{
			fmgr_info_cxt(s_type->typreceive, &type->io.in.recv_func, func->ctx);
			type->has_recv = 1;
		}
	}

	ReleaseSysCache(t_type);

	return type;
}


/* Convert a Datum to parameter for libpq */
char *
plproxy_send_type(ProxyType *type, Datum val, bool allow_bin, int *len, int *fmt)
{
	bytea	   *bin;
	char	   *res;

	Assert(type->for_send == 1);

	if (allow_bin && type->has_send)
	{
		bin = SendFunctionCall(&type->io.out.send_func, val);
		res = VARDATA(bin);
		*len = VARSIZE(bin) - VARHDRSZ;
		*fmt = 1;
	}
	else
	{
		res = OutputFunctionCall(&type->io.out.output_func, val);
		*len = 0;
		*fmt = 0;
	}
	return res;
}

/*
 * Point StringInfo to fixed buffer.
 *
 * Supposedly StringInfo wants 0 at the end.
 * Luckily libpq already provides it so all is fine.
 *
 * Although it should not matter to binary I/O functions.
 */
static void
setFixedStringInfo(StringInfo str, void *data, int len)
{
	str->data = data;
	str->maxlen = len;
	str->len = len;
	str->cursor = 0;
}

/* Convert a libpq result to Datum */
Datum
plproxy_recv_type(ProxyType *type, char *val, int len, bool bin)
{
	Datum		res;
	StringInfoData buf;

	Assert(type->for_send == 0);

	if (bin)
	{
		if (!type->has_recv)
			elog(ERROR, "PL/Proxy: type %u recv not supported", type->type_oid);

		/* avoid unnecessary copy */
		setFixedStringInfo(&buf, val, len);

		res = ReceiveFunctionCall(&type->io.in.recv_func,
								  &buf, type->io_param, -1);
	}
	else
	{
		res = InputFunctionCall(&type->io.in.input_func,
								val, type->io_param, -1);
	}
	return res;
}
