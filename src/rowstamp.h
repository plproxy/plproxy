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

