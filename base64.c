/*
 * Written by Poul-Henning Kamp <phk@phk.freebsd.dk>
 *
 * This file is in the public domain.
 */


#include <sys/types.h>
#include "base64.h"

static const char b64[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static char i64[256];

void
VB64_init(void)
{
	int i;
	const char *p;

	for (i = 0; i < 256; i++)
		i64[i] = -1;
	for (p = b64, i = 0; *p; p++, i++)
		i64[(int)*p] = (char)i;
	i64['='] = 0;
}

/**
 * A bit different from varnish VB64_decode():
 *  - takes a length constrained ('slen') input string 's'
 *  - returns the number of bytes decoded to 'd' (or -1 on error).
 *  - does not null-terminate the string.
 */
int VB64_decode2 (char *d, unsigned dlen, const char *s, int slen) {
	char *dbegin = d;
	const char *end = s + slen;
	unsigned u, v, l;
	int i;

	u = 0;
	l = 0;
	while (s < end) {
		for (v = 0; v < 4; v++) {
			if (s == end)
				break;
			i = i64[(int)*s++];
			if (i < 0)
				return (-1);
			u <<= 6;
			u |= i;
		}
		for (v = 0; v < 3; v++) {
			if (l >= dlen - 1)
				return (-1);
			*d = (u >> 16) & 0xff;
			u <<= 8;
			l++;
			d++;
		}
	}

	return (int)(d - dbegin);
}
