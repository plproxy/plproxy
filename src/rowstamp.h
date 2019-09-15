
static inline Oid XProcTupleGetOid(HeapTuple proc_tup) {
#if PG_VERSION_NUM >= 120000
	Form_pg_proc proc_struct;
	proc_struct = (Form_pg_proc) GETSTRUCT(proc_tup);
	return proc_struct->oid;
#else
	return HeapTupleGetOid(proc_tup);
#endif
}

static inline Oid XNamespaceTupleGetOid(HeapTuple ns_tup) {
#if PG_VERSION_NUM >= 120000
	Form_pg_namespace ns_struct;
	ns_struct = (Form_pg_namespace) GETSTRUCT(ns_tup);
	return ns_struct->oid;
#else
	return HeapTupleGetOid(ns_tup);
#endif
}

/*
 * Row version check
 */
typedef struct RowStamp {
	TransactionId		xmin;
	ItemPointerData		tid;
} RowStamp;

static inline void plproxy_set_stamp(RowStamp *stamp, HeapTuple tup)
{
	stamp->xmin = HeapTupleHeaderGetXmin(tup->t_data);
	stamp->tid = tup->t_self;
}

static inline bool plproxy_check_stamp(RowStamp *stamp, HeapTuple tup)
{
	return stamp->xmin == HeapTupleHeaderGetXmin(tup->t_data)
		&& ItemPointerEquals(&stamp->tid, &tup->t_self);
}

/*
 * System cache stamp
 */

typedef uint32 SCInvalArg;
typedef struct SysCacheStamp {
	uint32 cacheid;
	uint32 hashValue;
} SysCacheStamp;

static inline void scstamp_set(int cache, SysCacheStamp *stamp, Oid oid)
{
	stamp->cacheid = cache;
	stamp->hashValue = GetSysCacheHashValue1(cache, oid);
}

static inline bool scstamp_check(int cache, SysCacheStamp *stamp, uint32 hashValue)
{
	if (stamp->cacheid == 0)
		return true;
	if (cache != stamp->cacheid)
		elog(WARNING, "cache id mismatch: stamp:%d cur:%d", stamp->cacheid, cache);
	return !hashValue || stamp->hashValue == hashValue;
}

