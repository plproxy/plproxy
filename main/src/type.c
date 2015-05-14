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
	/*
	 * We need to properly track if remote server has:
	 * - same major:minor version
	 * - same server_encoding
	 * - same integer_timestamps
	 *
	 * Currently plproxy does the decision too early,
	 * thus no safe types are left.  Disable is totally,
	 * until lazy decision-making is possible.
	 */
	if (1)
		return false;

	switch (oid)
	{
		case BOOLOID:
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case FLOAT4OID:
		case FLOAT8OID:
		case NUMERICOID:
		case BYTEAOID:
			return true;

		/* client_encoding issue */
		case TEXTOID:
		case BPCHAROID:
		case VARCHAROID:

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

bool
plproxy_composite_valid(ProxyComposite *type)
{
	HeapTuple type_tuple;
	HeapTuple rel_tuple;
	Form_pg_type pg_type;
	Oid oid = type->tupdesc->tdtypeid;
	bool res;

	if (!type->alterable)
		return true;
	type_tuple = SearchSysCache(TYPEOID, ObjectIdGetDatum(oid), 0, 0, 0);
	if (!HeapTupleIsValid(type_tuple))
		elog(ERROR, "cache lookup failed for type %u", oid);

	pg_type = (Form_pg_type) GETSTRUCT(type_tuple);
	rel_tuple = SearchSysCache(RELOID, ObjectIdGetDatum(pg_type->typrelid), 0, 0, 0);
	if (!HeapTupleIsValid(rel_tuple))
		elog(ERROR, "cache lookup failed for type relation %u", pg_type->typrelid);

	res = plproxy_check_stamp(&type->stamp, rel_tuple);

	ReleaseSysCache(rel_tuple);
	ReleaseSysCache(type_tuple);

	return res;
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
	Oid oid = tupdesc->tdtypeid;

	old_ctx = MemoryContextSwitchTo(func->ctx);

	ret = palloc(sizeof(*ret));
	ret->type_list = palloc(sizeof(ProxyType *) * natts);
	ret->name_list = palloc0(sizeof(char *) * natts);
	ret->tupdesc = BlessTupleDesc(tupdesc);
	ret->use_binary = 1;

	ret->alterable = 0;
	if (oid != RECORDOID)
	{
		HeapTuple type_tuple;
		HeapTuple rel_tuple;
		Form_pg_type pg_type;

		type_tuple = SearchSysCache(TYPEOID, ObjectIdGetDatum(oid), 0, 0, 0);
		if (!HeapTupleIsValid(type_tuple))
			elog(ERROR, "cache lookup failed for type %u", oid);
		pg_type = (Form_pg_type) GETSTRUCT(type_tuple);
		rel_tuple = SearchSysCache(RELOID, ObjectIdGetDatum(pg_type->typrelid), 0, 0, 0);
		if (!HeapTupleIsValid(rel_tuple))
			elog(ERROR, "cache lookup failed for type relation %u", pg_type->typrelid);
		plproxy_set_stamp(&ret->stamp, rel_tuple);
		ReleaseSysCache(rel_tuple);
		ReleaseSysCache(type_tuple);
		ret->alterable = 1;

		if (ret->tupdesc->tdtypeid != oid)
			elog(ERROR, "lost oid");
	}

	MemoryContextSwitchTo(old_ctx);

	ret->nfields = 0;
	for (i = 0; i < natts; i++)
	{
		a = tupdesc->attrs[i];
		if (a->attisdropped)
		{
			ret->name_list[i] = NULL;
			ret->type_list[i] = NULL;
			continue;
		}
		ret->nfields++;

		name = quote_identifier(NameStr(a->attname));
		ret->name_list[i] = plproxy_func_strdup(func, name);

		type = plproxy_find_type_info(func, a->atttypid, 0);
		ret->type_list[i] = type;

		if (!type->has_recv)
			ret->use_binary = 0;
	}

	return ret;
}

void
plproxy_free_composite(ProxyComposite *rec)
{
	int i;
	int natts = rec->tupdesc->natts;

	for (i = 0; i < natts; i++)
	{
		plproxy_free_type(rec->type_list[i]);
		if (rec->name_list[i])
			pfree(rec->name_list[i]);
	}
	pfree(rec->type_list);
	pfree(rec->name_list);
	FreeTupleDesc(rec->tupdesc);
	pfree(rec);
}

void
plproxy_free_type(ProxyType *type)
{
	if (type == NULL)
		return;

	if (type->name)
		pfree(type->name);

	if (type->elem_type_t)
		plproxy_free_type(type->elem_type_t);

	/* hopefully I/O functions do not use ->fn_extra */

	pfree(type);
}

/*
 * Build result tuple from binary or CString values.
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
		{
			dvalues[i] = (Datum)NULL;
			nulls[i] = 'n';
			continue;
		}

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
			plproxy_error(func, "unsupported type code: %s (%u)", namebuf, oid);
			break;
		case TYPTYPE_PSEUDO:
			if (oid != VOIDOID)
				plproxy_error(func, "unsupported pseudo type: %s (%u)",
							  namebuf, oid);
			break;
		case TYPTYPE_BASE:
		case TYPTYPE_COMPOSITE:
		case TYPTYPE_DOMAIN:
		case TYPTYPE_ENUM:
		case TYPTYPE_RANGE:
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
	type->is_array = (s_type->typelem != 0 && s_type->typlen == -1);
	type->elem_type_oid = s_type->typelem;
	type->elem_type_t = NULL;
	type->alignment = s_type->typalign;
	type->length = s_type->typlen;

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

/* Get cached type info for array elems */
ProxyType *plproxy_get_elem_type(ProxyFunction *func, ProxyType *type, bool for_send)
{
	if (!type->elem_type_t)
		type->elem_type_t = plproxy_find_type_info(func, type->elem_type_oid, for_send);
	return type->elem_type_t;
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
