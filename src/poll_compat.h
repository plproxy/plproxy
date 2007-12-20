
#ifdef HAVE_POLL_H

#include <poll.h>

#else
#ifdef HAVE_SYS_POLL_H

#include <sys/poll.h>

#else

#include <sys/select.h>

#define POLLIN	(1 << 0)
#define POLLOUT	(1 << 1)
#define POLLHUP	(1 << 2)

#define pollfd compat_pollfd
#define poll compat_poll

struct pollfd {
	int fd;
	short events;
	short revents;
};

static int poll(struct pollfd *fds, unsigned nfds, int timeout_ms)
{
	struct pollfd *pf;
	int i, fd_max = 0;
	int res;
	fd_set r_set, w_set;

	FD_ZERO(&r_set);
	FD_ZERO(&w_set);
	for (i = 0; i < nfds; i++) {
		pf = fds + i;
		if (pf->fd < 0 || pf->fd >= FD_SETSIZE)
		{
			/* give different errno for FD_SETSIZE to allow detect it */
			errno = (pf->fd < 0) ? EBADF : EFAULT;
			return -1;
		}
		if (pf->events & POLLIN)
			FD_SET(pf->fd, &r_set);
		if (pf->events & POLLOUT)
			FD_SET(pf->fd, &w_set);
		if (pf->fd > fd_max)
			fd_max = pf->fd;
	}

	if (timeout_ms >= 0)
	{
		struct timeval tv;
		tv.tv_sec = timeout_ms / 1000;
		tv.tv_usec = timeout_ms % 1000;
		res = select(fd_max + 1, &r_set, &w_set, NULL, &tv);
	} else
		res = select(fd_max + 1, &r_set, &w_set, NULL, NULL);

	if (res <= 0)
		return res;

	for (i = 0; i < nfds; i++) {
		pf = fds + i;
		pf->revents = 0;
		if ((pf->events & POLLIN) && FD_ISSET(pf->fd, &r_set))
			pf->revents |= POLLIN;
		if ((pf->events & POLLOUT) && FD_ISSET(pf->fd, &w_set))
			pf->revents |= POLLOUT;
	}

	return res;
}

#endif /* !HAVE_SYS_POLL_H */
#endif /* !HAVE_POLL_H */


