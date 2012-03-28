/*
 * Row version check changed in 8.3
 */

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
	stamp->tid = tup->t_self;
}

static inline bool plproxy_check_stamp(RowStamp *stamp, HeapTuple tup)
{
	return stamp->xmin == HeapTupleHeaderGetXmin(tup->t_data)
		&& ItemPointerEquals(&stamp->tid, &tup->t_self);
}

#endif

/*
 * SyscacheCallback check changed in 9.2.
 */

#if PG_VERSION_NUM >= 90200

typedef uint32 SCInvalArg;
typedef struct SysCacheStamp {
	uint32 cacheid;
	uint32 hashValue;
} SysCacheStamp;

static inline void scstamp_set(int cache, SysCacheStamp *stamp, HeapTuple tup)
{
	Oid oid = HeapTupleGetOid(tup);
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

#else

/*
 * Pre-9.2 cache invalidation.
 */

typedef ItemPointer SCInvalArg;
typedef struct SysCacheStamp {
	ItemPointerData     tupleId;
} SysCacheStamp;

static inline void scstamp_set(int cache, SysCacheStamp *stamp, HeapTuple tup)
{
	stamp->tupleId = tup->t_self;
}

static inline bool scstamp_check(int cache, SysCacheStamp *stamp, ItemPointer scrow)
{
	return !scrow || ItemPointerEquals(&stamp->tupleId, scrow);
}

#endif

