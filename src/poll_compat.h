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

#ifndef POLL_COMPAT_H
#define POLL_COMPAT_H

/* define to test poll() compat function */
#if 0
#define PLPROXY_POLL_COMPAT 
#endif

#include <sys/time.h>

/* see if real poll() can be used */
#ifndef PLPROXY_POLL_COMPAT
#ifdef HAVE_POLL_H
#include <poll.h>
#else
#ifdef HAVE_SYS_POLL_H
#include <sys/poll.h>
#else
#define PLPROXY_POLL_COMPAT
#endif
#endif
#endif

/*
 * Emulate poll() with select(), if needed.
 */
#ifdef PLPROXY_POLL_COMPAT

/* in/out event types */
#define POLLIN		(1 << 0)
#define POLLOUT		(1 << 1)

/* rest are unused in this implementation */
#define POLLHUP		(1 << 2)
#define POLLPRI		(1 << 3)
#define POLLNVAL	(1 << 4)
#define POLLERR		(1 << 5)

/* avoid namespace conflicts */
#define pollfd	plproxy_compat_pollfd
#define poll	plproxy_compat_poll
#define nfds_t	plproxy_compat_nfds_t

struct pollfd {
	int fd;
	short events;
	short revents;
};

typedef unsigned long nfds_t;

int poll(struct pollfd *fds, nfds_t nfds, int timeout_ms);

#endif /* PLPROXY_POLL_COMPAT */

#endif /* POLL_COMPAT_H */

