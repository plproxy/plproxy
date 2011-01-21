
#include "postgres.h"

#include "poll_compat.h"

#ifdef PLPROXY_POLL_COMPAT

/*
 * Emulate poll() with select()
 */

#include <sys/time.h>

#ifdef HAVE_SYS_SELECT_H
#include <sys/select.h>
#endif

/*
 * dynamic buffer for fd_set to avoid depending on FD_SETSIZE
 */

struct fd_buf {
	fd_set *set;
	int alloc_bytes;
};

static void fdbuf_zero(struct fd_buf *buf)
{
	if (buf->set)
		memset(buf->set, 0, buf->alloc_bytes);
}

static bool fdbuf_resize(struct fd_buf *buf, int fd)
{
	int need_bytes;
	unsigned char *ptr;
	/* default allocation */
	int alloc = sizeof(fd_set);

#ifdef WIN32
	int cnt = buf->set ? buf->set->fd_count : 0;
	/* win32 fd_set is array of handles, +8 for count&padding */
	need_bytes = (cnt + 1) * sizeof(buf->set->fd_array[0]) + 8;
#else
	/* otherwise, fd_set is bitmap, +8 for int/long alignment */
	need_bytes = fd / 8 + 8;
#endif

	if (buf->alloc_bytes < need_bytes)
	{
		while (alloc < need_bytes)
			alloc *= 2;

		if (!buf->set)
			ptr = malloc(alloc);
		else
			ptr = realloc(buf->set, alloc);

		if (!ptr)
			return false;

		/* clean new area */
		memset(ptr + buf->alloc_bytes, 0, alloc - buf->alloc_bytes);

		buf->set = (fd_set *)ptr;
		buf->alloc_bytes = alloc;
	}
	return true;
}

/* win32: make macros ignore FD_SETSIZE */
#undef FD_SETSIZE
#define FD_SETSIZE (1 << 30)

int poll(struct pollfd *fds, nfds_t nfds, int timeout_ms)
{
	static struct fd_buf readfds = { NULL, 0 };
	static struct fd_buf writefds = { NULL, 0 };

	struct pollfd *pf;
	int res, fd_max = 0;
	struct timeval *tv = NULL;
	struct timeval tvreal;
	unsigned i;

	/* convert timeout_ms to timeval */
	if (timeout_ms >= 0)
	{
		tvreal.tv_sec = timeout_ms / 1000;
		tvreal.tv_usec = (timeout_ms % 1000) * 1000;
		tv = &tvreal;
	} else if (timeout_ms < -1)
		goto err_inval;

	/*
	 * Convert pollfds to fd sets.
	 */
	fdbuf_zero(&readfds);
	fdbuf_zero(&writefds);
	for (i = 0; i < nfds; i++)
	{
		pf = fds + i;
		if (pf->fd < 0)
			goto err_badf;

		/* sets must be equal size */
		if (!fdbuf_resize(&readfds, pf->fd))
			goto err_nomem;
		if (!fdbuf_resize(&writefds, pf->fd))
			goto err_nomem;

		if (pf->events & POLLIN)
			FD_SET((unsigned)pf->fd, readfds.set);
		if (pf->events & POLLOUT)
			FD_SET((unsigned)pf->fd, writefds.set);
		if (pf->fd > fd_max)
			fd_max = pf->fd;
	}

	res = select(fd_max + 1, readfds.set, writefds.set, NULL, tv);
	if (res <= 0)
		return res;

	/*
	 * select() and poll() count fd-s differently,
	 * need to recount them here.
	 */
	res = 0;

	for (i = 0; i < nfds; i++)
	{
		pf = fds + i;
		pf->revents = 0;
		if ((pf->events & POLLIN) && FD_ISSET(pf->fd, readfds.set))
			pf->revents |= POLLIN;
		if ((pf->events & POLLOUT) && FD_ISSET(pf->fd, writefds.set))
			pf->revents |= POLLOUT;
		if (pf->revents)
			res += 1;
	}
	return res;

err_nomem:
	errno = ENOMEM;
	return -1;

err_badf:
	errno = EBADF;
	return -1;
err_inval:
	errno = EINVAL;
	return -1;
}

#endif /* PLPROXY_POLL_COMPAT */

