
#define MALLOCLOGx

static inline void *my_malloc(const char *pfx, int len) {
	void *p = palloc(len);
#ifdef MALLOCLOG
	elog(NOTICE, "%s:%s(%d) = %p", pfx, __FUNCTION__, len, p);
#endif
	return p;
}
static inline void *my_realloc(const char *pfx, void *old, int len) {
	void *p = repalloc(old, len);
#ifdef MALLOCLOG
	elog(NOTICE, "%s:%s(%p, %d) = %p", pfx, __FUNCTION__, old, len, p);
#endif
	return p;
}
static inline void my_free(const char *pfx, void *p) {
	pfree(p);
#ifdef MALLOCLOG
	elog(NOTICE, "%s:%s(%p)", pfx, __FUNCTION__, p);
#endif
}

#define malloc(x)  my_malloc(__FILE__, x)
#define realloc(x, y)  my_realloc(__FILE__, x, y)
#define free(x)  my_free(__FILE__, x)

