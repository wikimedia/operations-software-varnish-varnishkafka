/*
 * varnishkafka
 *
 * Copyright (c) 2013 Wikimedia Foundation
 * Copyright (c) 2013 Magnus Edenhill <vk@edenhill.se>
 *
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met: 
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer. 
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution. 
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE 
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF 
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS 
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN 
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */


#define _XOPEN_SOURCE 500    /* for strptime() */
#define _BSD_SOURCE          /* for daemon() */
#define _GNU_SOURCE          /* for strndupa() */
#include <ctype.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <unistd.h>
#include <string.h>
#include <time.h>
#include <assert.h>
#include <errno.h>
#include <sys/queue.h>
#include <syslog.h>
#include <netdb.h>

#include <varnish/varnishapi.h>
#include <librdkafka/rdkafka.h>

#include <yajl/yajl_common.h>
#include <yajl/yajl_gen.h>
#include <yajl/yajl_version.h>

#include "varnishkafka.h"
#include "base64.h"


/* Kafka handle */
static rd_kafka_t *rk;
/* Kafka topic */
static rd_kafka_topic_t *rkt;

/* Varnish shared memory handle*/
struct VSM_data *vd;

const char *conf_file_path = VARNISHKAFKA_CONF_PATH;


/**
 * Currently parsed logline(s)
 */
static struct logline **loglines;
static int              logline_cnt;



static const char *fmt_conf_names[] = {
	[FMT_CONF_MAIN] = "Main",
	[FMT_CONF_KEY]  = "Key"
};

/**
 * All constant strings in the format are placed in 'const_string' which
 * hopefully will be small enough to fit a single cache line.
 */
static char  *const_string      = NULL;
static size_t const_string_size = 0;
static size_t const_string_len  = 0;

/**
 * Adds a constant string to the constant string area.
 * If the string is already found in the area, return it instead.
 */
static char *const_string_add (const char *in, int inlen) {
	char *ret;
	const char *instr = strndupa(in, inlen);

	if (!const_string || !(ret = strstr(const_string, instr))) {
		if (const_string_len + inlen + 1 >= const_string_size) {
			/* Reallocate buffer to fit new string (and more) */
			const_string_size = (const_string_size + inlen + 64)*2;
			const_string = realloc(const_string, const_string_size);
		}

		/* Append new string */
		ret = const_string + const_string_len;
		memcpy(ret, in, inlen);
		ret[inlen] = '\0';
		const_string_len += inlen;
	}

	return ret;
}


/**
 * Print parsed format string: formatters
 */
static __attribute__((unused)) void fmt_dump (const struct fmt_conf *fconf) {
	int i;

	_DBG("%s %i/%i formats:",
	     fmt_conf_names[fconf->fid], fconf->fmt_cnt, fconf->fmt_size);
	for (i = 0 ; i < fconf->fmt_cnt ; i++) {
		_DBG(" #%-3i  fmt %i (%c)  var \"%s\", def (%i)\"%.*s\"%s",
		     i,
		     fconf->fmt[i].id,
		     isprint(fconf->fmt[i].id) ? (char)fconf->fmt[i].id : ' ',
		     fconf->fmt[i].var ? : "",
		     fconf->fmt[i].deflen, fconf->fmt[i].deflen,
		     fconf->fmt[i].def,
		     fconf->fmt[i].flags & FMT_F_ESCAPE ? ", escape" : "");
	}
}

/**
 * Print parser format string: tags
 */
static __attribute__((unused)) void tag_dump (void) {
	int i;

	_DBG("Tags:");
	for (i = 0 ; i < VSL_TAGS_MAX ; i++) {
		struct tag *tag;

		for (tag = conf.tag[i] ; tag ; tag = tag->next) {
			_DBG(" #%-3i  spec 0x%x, tag %s (%i), var \"%s\", "
			     "parser %p, col %i, fmt #%i %i (%c)",
			     i,
			     tag->spec,
			     VSL_tags[tag->tag], tag->tag,
			     tag->var, tag->parser,
			     tag->col,
			     tag->fmt->idx,
			     tag->fmt->id,
			     isprint(tag->fmt->id) ?
			     (char)tag->fmt->id : 0);
		}
	}
}



/**
 * Adds a parsed formatter to the list of formatters
 */
static int format_add (struct fmt_conf *fconf, int fmtr,
		       const char *var, ssize_t varlen,
		       const char *def, ssize_t deflen,
		       int flags,
		       char *errstr, size_t errstr_size) {
	struct fmt *fmt;

	if (fconf->fmt_cnt >= fconf->fmt_size) {
		fconf->fmt_size = (fconf->fmt_size ? : 32) * 2;
		fconf->fmt = realloc(fconf->fmt,
				     fconf->fmt_size * sizeof(*fconf->fmt));
	}

	fmt = &fconf->fmt[fconf->fmt_cnt];
	memset(fmt, 0, sizeof(*fmt));

	fmt->id    = fmtr;
	fmt->idx   = fconf->fmt_cnt;
	fmt->flags = flags;
	if (var) {
		if (varlen == -1)
			varlen = strlen(var);

		fmt->var = malloc(varlen+1);
		memcpy((char *)fmt->var, var, varlen);
		((char *)fmt->var)[varlen] = '\0';
	} else
		fmt->var = NULL;

	if (!def)
		def = "-";

	if (deflen == -1)
		deflen = strlen(def);
	fmt->deflen = deflen;
	fmt->def = const_string_add(def, deflen);

	fconf->fmt_cnt++;

	return fmt->idx;
}



/**
 * Adds a parsed tag to the list of tags
 */
static int tag_add (struct fmt_conf *fconf, struct fmt *fmt,
		    int spec, int tagid,
		    const char *var, ssize_t varlen,
		    int col,
		    int (*parser) (const struct tag *tag, struct logline *lp,
				   const char *ptr, int len),
		    int tag_flags,
		    char *errstr, size_t errstr_size) {
	struct tag *tag;

	tag = calloc(1, sizeof(*tag));

	assert(tagid < VSL_TAGS_MAX);

	if (conf.tag[tagid])
		tag->next = conf.tag[tagid];

	conf.tag[tagid] = tag;

	tag->spec   = spec;
	tag->tag    = tagid;
	tag->col    = col;
	tag->fmt    = fmt;
	tag->parser = parser;
	tag->flags  = tag_flags;
	tag->fid    = fconf->fid;

	if (var) {
		if (varlen == -1)
			varlen = strlen(var);

		tag->var = malloc(varlen+1);
		tag->varlen = varlen;
		memcpy(tag->var, var, varlen);
		tag->var[varlen] = '\0';
	} else
		tag->var = NULL;

	return 0;
}
		     


static inline void match_assign0 (const struct tag *tag, struct logline *lp,
				  const char *ptr, int len);
static void match_assign (const struct tag *tag, struct logline *lp,
			  const char *ptr, int len);




/**
 * Returns true if 'ptr' is within 'lp's scratch pad, else false.
 */
static inline int is_scratch_ptr (const struct logline *lp, const char *ptr) {
	return (lp->scratch <= ptr && ptr < lp->scratch + sizeof(lp->scratch));
}

/**
 * Rewinds (deallocates) the last allocation by 'len' bytes.
 */
static inline void scratch_rewind (struct logline *lp, int len) {
	assert(lp->sof >= len);
	lp->sof -= len;
}


/**
 * Allocate persistent memory space ('len' bytes) in 
 * logline 'lp's scratch buffer.
 */
static inline char *scratch_alloc (const struct tag *tag, struct logline *lp,
				   int len) {
	char *ptr;

	if (lp->sof + len > sizeof(lp->scratch)) {
		vk_log("WARNING", LOG_WARNING,
		       "scratch pad is too small (%zd bytes), "
		       "need %i bytes or more",
		       sizeof(lp->scratch), lp->sof + len);
		return NULL;
	}

	ptr = lp->scratch + lp->sof;
	lp->sof += len;
	return ptr;
}

/**
 * Helper that allocates 'len' bytes in the scratch buffer and
 * writes the contents of 'src' there.
 */
static inline int scratch_write (const struct tag *tag, struct logline *lp,
				 const char *src, int len) {
	char *dst;

	if (unlikely((dst = scratch_alloc(tag, lp, len)) == NULL))
		return -1;

	memcpy(dst, src, len);

	match_assign(tag, lp, dst, len);

	return len;
}

/**
 * Same as scratch_write0() but calls match_assign0() directly, thus
 * not supporting escaping.
 */
static inline int scratch_write0 (const struct tag *tag, struct logline *lp,
				 const char *src, int len) {
	char *dst;

	if (unlikely((dst = scratch_alloc(tag, lp, len)) == NULL))
		return -1;

	memcpy(dst, src, len);

	match_assign0(tag, lp, dst, len);

	return len;
}


/**
 * Writes 'src' of 'len' bytes to scratch buffer, escaping
 * all unprintable characters as well as the ones defined in 'map' below.
 * Returns -1 on error.
 */
static inline int scratch_write_escaped (const struct tag *tag,
					 struct logline *lp,
					 const char *src, int len) {
	static const char *map[256] = {
		['\t'] = "\\t",
		['\n'] = "\\n",
		['\r'] = "\\r",
		['\v'] = "\\v",
		['\f'] = "\\f",
		['"']  = "\\\"",
		[' ']  = "\\ ",
	};
	char *dst;
	char *dstend;
	char *d;
	const char *s, *srcend = src + len;

	/* Allocate initial space for escaped string. */
	if (unlikely((dst = scratch_alloc(tag, lp, len + 10)) == NULL))
		return -1;

	dstend = dst + len + 10;

	s = src;
	d = dst;
	while (s < srcend) {
		int outlen = 1;
		const char *out;
		char tmp[6];

		if (unlikely((out = map[(int)*s]) != NULL)) {
			/* Escape from 'map' */
			outlen = 2;

		} else if (unlikely(!isprint(*s))) {
			/* Escape non-printables as \<octal> */
			sprintf(tmp, "\%04o", (int)*s);
			out = tmp;
			outlen = 5;

		} else {
			/* No escaping */
			out = s;
		}

		/* Increase scratch pad if necessary. */
		if (unlikely((d + outlen >= dstend))) {
			if (unlikely(!scratch_alloc(tag, lp, outlen + 20)))
				return -1;
			dstend += outlen + 20;
		}

		if (likely(outlen == 1))
			*(d++) = *out;
		else {
			memcpy(d, out, outlen);
			d += outlen;
		}

		s++;
	}

	/* Rewind scratch pad to reclaim unused memory. */
	scratch_rewind(lp, (int)(dstend-d));

	/* Assign new matched string */
	match_assign0(tag, lp, dst, (int)(d-dst));

	return 0;
}

/**
 * Helper that allocates enough space in the scratch buffer to fit
 * the string produced by 'fmt...'.
 */
static inline int scratch_printf (const struct tag *tag, struct logline *lp,
				  const char *fmt, ...) {
	va_list ap, ap2;
	int r;
	char *dst;

	va_copy(ap2, ap);
	va_start(ap2, fmt);
	r = vsnprintf(NULL, 0, fmt, ap2);
	va_end(ap2);

	if (!(dst = scratch_alloc(tag, lp, r+1)))
		return -1;

	va_start(ap, fmt);
	vsnprintf(dst, r+1, fmt, ap);
	va_end(ap);

	match_assign(tag, lp, dst, r);

	return r;
}

static inline int scratch_printf (const struct tag *tag, struct logline *lp,
				  const char *fmt, ...)
	__attribute__((format(printf,3,4)));




static inline void match_assign0 (const struct tag *tag, struct logline *lp,
				  const char *ptr, int len) {
	assert(len >= 0);
	lp->match[tag->fid][tag->fmt->idx].ptr = ptr;
	lp->match[tag->fid][tag->fmt->idx].len = len;
}


/**
 * Assign 'PTR' of size 'LEN' as a match for 'TAG' in logline 'LP'.
 *
 * 'PTR' must be a pointer to persistent memory:
 *   - either VSL shared memory (original VSL tag payload)
 *   - or to a buffer allocated in the 'LP' scratch buffer.
 */
static void match_assign (const struct tag *tag, struct logline *lp,
			  const char *ptr, int len) {

	if (unlikely(tag->fmt->flags & FMT_F_ESCAPE)) {
		/* If 'ptr' is in the scratch pad; rewind the scratch pad
		 * since we'll be re-writing the string escaped. */
		if (is_scratch_ptr(lp, ptr)) {
			ptr = strndupa(ptr, len);
			scratch_rewind(lp, len);
		}
		scratch_write_escaped(tag, lp, ptr, len);

	} else {
		if (conf.datacopy) /* copy volatile data */
			scratch_write0(tag, lp, ptr, len);
		else  /* point to persistent data */
			match_assign0(tag, lp, ptr, len);
	}
}



static char *strnchr (const char *s, int len, int c) {
	const char *end = s + len;
	while (s < end) {
		if (*s == c)
			return (char *)s;
		s++;
	}

	return NULL;
}


/**
 * Looks for any matching character from 'match' in 's' and returns
 * a pointer to the first match, or NULL if none of 'match' matched 's'.
 */
static char *strnchrs (const char *s, int len, const char *match) {
	const char *end = s + len;
	char map[256] = {};
	while (*match)
		map[(int)*(match++)] = 1;
	
	while (s < end) {
		if (map[(int)*s])
			return (char *)s;
		s++;
	}

	return NULL;
}
	

/**
 * Splits 'ptr' (with length 'len') by delimiter 'delim' and assigns
 * the Nth ('col') column to '*dst' and '*dstlen'.
 * Does not modify the input data ('ptr'), only points to it.
 *
 * Returns 1 if the column was found, else 0.
 *
 * NOTE: Columns start at 1.
 */
static int column_get (int col, char delim, const char *ptr, int len,
		       const char **dst, int *dstlen) {
	const char *s = ptr;
	const char *b = s;
	const char *end = s + len;
	int i = 0;

	while (s < end) {
		if (*s != delim) {
			s++;
			continue;
		}

		if (s != b && col == ++i) {
			*dst = b;
			*dstlen = (int)(s - b);
			return 1;
		}

		b = ++s;
	}

	if (s != b && col == ++i) {
		*dst = b;
		*dstlen = (int)(s - b);
		return 1;
	}

	return 0;
}



/**
 *
 * Misc parsers for formatters
 *
 */
static int parse_BackendOpen (const struct tag *tag, struct logline *lp,
			      const char *ptr, int len) {
	const char *s;
	int slen;
	const int deflen = strlen("default");

	if (unlikely(!column_get(1, ' ', ptr, len, &s, &slen)))
		return 0;

	if (slen == deflen && !strncmp(s, "default", slen))
		column_get(2, ' ', ptr, len, &s, &slen);

	match_assign(tag, lp, s, slen);

	return 0;
}	

static int parse_U (const struct tag *tag, struct logline *lp,
		    const char *ptr, int len) {
	const char *qs;
	int slen = len;

	if ((qs = strnchr(ptr, len, '?')))
		slen = (int)(qs - ptr);

	match_assign(tag, lp, ptr, slen);
	return slen;
}

static int parse_q (const struct tag *tag, struct logline *lp,
		    const char *ptr, int len) {
	const char *qs;
	int slen = len;

	if (!(qs = strnchr(ptr, len, '?')))
		return 0;

	slen = len - (int)(qs - ptr);

	match_assign(tag, lp, qs, slen);
	return slen;
}

static int parse_t (const struct tag *tag, struct logline *lp,
		    const char *ptr, int len) {
	struct tm tm;
	char *dst;
	const char *timefmt = "[%d/%b/%Y:%T %z]";
	const int timelen   = 64;
	int tlen;

	/* Use config-supplied time formatting */
	if (tag->var)
		timefmt = tag->var;

	if (tag->tag == SLT_TxHeader) {
		if (unlikely(!strptime(strndupa(ptr, len),
				       "%a, %d %b %Y %T", &tm)))
			return 0;

	} else {
		time_t t = strtoul(ptr, NULL, 10);
		localtime_r(&t, &tm);
	}

	if (unlikely(!(dst = scratch_alloc(tag, lp, timelen))))
		return -1;

	/* Format time string */
	tlen = strftime(dst, timelen, timefmt, &tm);

	/* Redeem unused space */
	if (likely(tlen < timelen))
		scratch_rewind(lp, timelen-tlen);

	match_assign(tag, lp, dst, tlen);

	return tlen;
}

static int parse_auth_user (const struct tag *tag, struct logline *lp,
			    const char *ptr, int len) {
	int rlen = len - 6/*"basic "*/;
	int ulen;
	char *tmp;
	char *q;

	if (unlikely(rlen <= 0 || strncasecmp(ptr, "basic ", 6) || (rlen % 2)))
		return 0;

	/* Calculate base64 decoded length */
	if (unlikely(!(ulen = (rlen * 4) / 3)))
		return 0;

	/* Protect our stack */
	if (unlikely(ulen > 1000))
		return 0;

	tmp = alloca(ulen+1);

	if ((ulen = VB64_decode2(tmp, ulen, ptr+6, rlen)) <= 0)
		return 0;

	/* Strip password */
	if ((q = strnchr(tmp, ulen, ':')))
		*q = '\0';

	return scratch_write(tag, lp, tmp, strlen(tmp));
}


static int parse_hitmiss (const struct tag *tag, struct logline *lp,
			  const char *ptr, int len) {
	if (len == 3 && !strncmp(ptr, "hit", 3)) {
		match_assign(tag, lp, ptr, len);
		return len;
	} else if (len == 4 &&
		 (!strncmp(ptr, "miss", 4) ||
		  !strncmp(ptr, "pass", 4))) {
		match_assign(tag, lp, "miss", 4);
		return 4;
	}

	return 0;
}

static int parse_handling (const struct tag *tag, struct logline *lp,
			   const char *ptr, int len) {
	if ((len == 3 && !strncmp(ptr, "hit", 3)) ||
	    (len == 4 && (!strncmp(ptr, "miss", 4) ||
			  !strncmp(ptr, "pass", 4)))) {
		match_assign(tag, lp, ptr, len);
		return len;
	}
	return 0;
}

static int parse_seq (const struct tag *tag, struct logline *lp,
		       const char *ptr, int len) {
	return scratch_printf(tag, lp, "%"PRIu64, conf.sequence_number);
}



/**
 * 'arr' is an array of tuples (const char *from, const char *to) with
 * replacements. 'arr' must be terminated with a NULL (from).
 *
 * Returns a new allocated string with strings replaced according to 'arr'.
 *
 * NOTE: 'arr' must be sorted by descending 'from' length.
 */
static char *string_replace_arr (const char *in, const char **arr) {
	char  *out;
	size_t inlen = strlen(in);
	size_t outsize = (inlen + 64) * 2;
	size_t of = 0;
	const char *s, *sp;

	out = malloc(outsize);

	s = sp = in;
	while (*s) {
		const char **a;

		for (a = arr ; *a ; a += 2) {
			const char *from = arr[0];
			const char *to   = arr[1];
			size_t fromlen   = strlen(from);
			size_t tolen;
			ssize_t diff;

			if (strncmp(s, from, fromlen))
				continue;

			tolen = strlen(to);
			diff  = tolen - fromlen;

			if (s > sp) {
				memcpy(out+of, sp, (int)(s-sp));
				of += (int)(s-sp);
			}
			sp = s += fromlen;

			if (of + diff >= outsize) {
				/* Not enough space in output buffer,
				 * reallocate and make some headroom to
				 * avoid future reallocs. */
				outsize = (of + diff + 64) * 2;
				out = realloc(out, outsize);
				assert(out);
			}
			
			memcpy(out+of, to, tolen);
			of += tolen;
			s--;
			break;
		}
		s++;
	}

	if (s > sp) {
		memcpy(out+of, sp, (int)(s-sp));
		of += (int)(s-sp);
	}

	out[of] = '\0';

	return out;
}


/**
 * Parse the format string and build a parsing array.
 */
static int format_parse (struct fmt_conf *fconf, const char *format_orig,
			 char *errstr, size_t errstr_size) {
	/**
	 * Maps a formatter %X to a VSL tag and column id, or parser, or both
	 */
	struct {
		/* A formatter may be backed by multiple tags.
		 * The first matching tag observed in the log will be used. */
		struct {
			/* VSL_S_CLIENT or VSL_S_BACKEND, or both */
			int spec;
			/* The SLT_.. tag id */
			int tag;
			/* For "Name: Value" tags (such as SLT_RxHeader),
			 * this is the "Name" part. */
			const char *var;
			/* Special handling for non-name-value vars such as
			 * %{Varnish:handling}x. fmtvar is "Varnish:handling" */
			const char *fmtvar; 
			/* Column to extract:
			 * 0 for entire string, else 1, 2, .. */
			int col;
			/* Parser to manually extract and/or convert a
			 * tag's content. */
			int (*parser) (const struct tag *tag,
				       struct logline *lp,
				       const char *ptr, int len);
			/* Optional tag->flags */
			int tag_flags;
		} f[3+1]; /* increase size when necessary (max used size + 1) */
		
		/* Default string if no matching tag was found or all
		 * parsers failed, defaults to "-". */
		const char *def;

	} map[256] = {
		/* Indexed by formatter character as
		 * specified by varnishncsa(1) */
		['b'] = { {
				{ VSL_S_CLIENT, SLT_Length },
				{ VSL_S_BACKEND, SLT_RxHeader,
				  var: "content-length" }
			} },
		['H'] = { {
				{ VSL_S_CLIENT, SLT_RxProtocol },
				{ VSL_S_BACKEND, SLT_TxProtocol },
			}, def: "HTTP/1.0" },
		['h'] = { {
				{ VSL_S_CLIENT, SLT_ReqStart, col: 1 },
				{ VSL_S_BACKEND, SLT_BackendOpen,
				  parser: parse_BackendOpen }
			} },
		['i'] = { { 
				{ VSL_S_CLIENT, SLT_RxHeader },
			} },
		['l'] = { {
				{ VSL_S_CLIENT|VSL_S_BACKEND },
			}, def: conf.logname },
		['m'] = { {
				{ VSL_S_CLIENT, SLT_RxRequest },
				{ VSL_S_BACKEND, SLT_TxRequest },
			} },
		['q'] = { {
				{ VSL_S_CLIENT, SLT_RxURL, parser: parse_q },
				{ VSL_S_BACKEND, SLT_TxURL, parser: parse_q },
			},  def: "" },
		['o'] = { { 
				{ VSL_S_CLIENT, SLT_TxHeader },
			} },
		['s'] = { {
				{ VSL_S_CLIENT, SLT_TxStatus },
				{ VSL_S_BACKEND, SLT_RxStatus },
			} },
		['t'] = { {
				{ VSL_S_CLIENT, SLT_ReqEnd,
				  parser: parse_t, col: 3,
				  tag_flags: TAG_F_NOVARMATCH },
				{ VSL_S_BACKEND, SLT_RxHeader,
				  var: "date", parser: parse_t,
				  tag_flags: TAG_F_NOVARMATCH },
			} },
		['U'] = { {
				{ VSL_S_CLIENT, SLT_RxURL, parser: parse_U },
				{ VSL_S_BACKEND, SLT_TxURL, parser: parse_U },
			} },
		['u'] = { {
				{ VSL_S_CLIENT, SLT_RxHeader,
				  var: "authorization",
				  parser: parse_auth_user },
				{ VSL_S_BACKEND, SLT_TxHeader,
				  var: "authorization",
				  parser: parse_auth_user },
			} },
		['x'] = { { 
				{ VSL_S_CLIENT, SLT_ReqEnd,
				  fmtvar: "Varnish:time_firstbyte", col: 5 },
				{ VSL_S_CLIENT, SLT_VCL_call,
				  fmtvar: "Varnish:hitmiss",
				  parser: parse_hitmiss },
				{ VSL_S_CLIENT, SLT_VCL_call,
				  fmtvar: "Varnish:handling",
				  parser: parse_handling },

			} },
		['n'] = { {
				{ VSL_S_CLIENT|VSL_S_BACKEND, VSL_TAG__ONCE,
				  parser: parse_seq },
			} },
	};
	/* Replace legacy formatters */
	static const char *replace[] = {
		/* "legacy-formatter", "new-formatter(s)" */
		"%r", "%m http://%{Host?localhost}i%U%q %H",
		NULL, NULL
	};
	const char *s, *t;
	const char *format;
	int cnt = 0;

	/* Perform legacy replacements. */
	format = string_replace_arr(format_orig, replace);

	/* Parse the format string */
	s = t = format;
	while (*s) {
		const char *begin;
		const char *var = NULL;
		int varlen = 0;
		const char *def = NULL;
		int deflen = -1;
		const char *name = NULL;
		int namelen = -1;
		int fmtid;
		int i;
		int flags = 0;
		int type = FMT_TYPE_STRING;

		if (*s != '%') {
			s++;
			continue;
		}

		/* ".....%... "
		 *  ^---^  add this part as verbatim string */
		if (s > t)
			if (format_add(fconf, 0,
				       NULL, 0,
				       t, (int)(s - t),
				       0, errstr, errstr_size) == -1)
				return -1;

		begin = s;
		s++;

		/* Parse '{VAR}X': '*s' will be set to X, and 'var' to VAR.
		 * varnishkafka also adds the following features:
		 *
		 *  VAR?DEF    where DEF is a default value, in this mode
		 *             VAR can be empty, and {?DEF} may be applied to
		 *             any formatter.
		 *             I.e.: %{Content-type?text/html}o
		 *                   %{?no-user}u
		 *
		 *  VAR!OPTION Various formatting options, see below.
		 *
		 * Where OPTION is one of:
		 *  escape     Escape rogue characters in the value.
		 *             VAR can be empty and {!escape} may be applied to
		 *             any formatter.
		 *             I.e. %{User-Agent!escape}i
		 *                  %{?nouser!escape}u
		 *
		 * ?DEF and !OPTIONs can be combined.
		 */
		if (*s == '{') {
			const char *a = s+1;
			const char *b = strchr(a, '}');
			const char *q;

			if (!b) {
				snprintf(errstr, errstr_size,
					 "Expecting '}' after \"%.*s...\"",
					 30, begin);
				return -1;
			}

			if (a == b) {
				snprintf(errstr, errstr_size,
					 "Empty {} identifier at \"%.*s...\"",
					 30, begin);
				return -1;
			}

			if (!*(b+1)) {
				snprintf(errstr, errstr_size,
					 "No formatter following "
					 "identifier at \"%.*s...\"",
					 30, begin);
				return -1;
			}

			var = a;

			/* Check for @NAME, ?DEF and !OPTIONs */
			if ((q = strnchrs(a, (int)(b-a), "@?!"))) {
				const char *q2 = q;

				varlen = (int)(q - a);
				if (varlen == 0)
					var = NULL;

				/* Scan all ?DEF and !OPTIONs */
				do {
					int qlen;

					q++;

					if ((q2 = strnchrs(q, (int)(b-q2-1),
							   "@?!")))
						qlen = (int)(q2-q);
					else
						qlen = (int)(b-q);

					switch (*(q-1))
					{
					case '@':
						/* Output format field name */
						name = q;
						namelen = qlen;
						break;
					case '?':
						/* Default value */
						def = q;
						deflen = qlen;
						break;
					case '!':
						/* Options */
						if (!strncasecmp(q, "escape",
								 qlen))
							flags |= FMT_F_ESCAPE;
						else if (!strncasecmp(q, "num",
								      qlen))
							type = FMT_TYPE_NUMBER;
						else {
							snprintf(errstr,
								 errstr_size,
								 "Unknown "
								 "formatter "
								 "option "
								 "\"%.*s\" at "
								 "\"%.*s...\"",
								 qlen, q,
								 30, a);
							return -1;
						}
						break;
					}

				} while ((q = q2));

			} else
				varlen = (int)(b-a);			

			s = b+1;
		}

		if (!map[(int)*s].f[0].spec) {
			snprintf(errstr, errstr_size,
				 "Unknown formatter '%c' at \"%.*s...\"",
				 *s, 30, begin);
			return -1;
		}

		if (!def) {
			if (type == FMT_TYPE_NUMBER)
				def = "0";
			else
				def = map[(int)*s].def;
		}

		/* Add formatter to ordered list of formatters */
		if ((fmtid = format_add(fconf, *s, var, varlen,
					def, deflen, flags,
					errstr, errstr_size)) == -1)
			return -1;

		fconf->fmt[fmtid].type = type;

		if (name) {
			fconf->fmt[fmtid].name = name;
			fconf->fmt[fmtid].namelen = namelen;
		}

		cnt++;

		/* Now add the matched tags specification to the
		 * list of parse tags */
		for (i = 0 ; map[(int)*s].f[i].spec ; i++) {
			if (map[(int)*s].f[i].tag == 0)
				continue;

			/* mapping has fmtvar specified, make sure it 
			 * matches the format's variable. */
			if (map[(int)*s].f[i].fmtvar) {
				if (!var ||
				    strlen(map[(int)*s].f[i].fmtvar) != varlen||
				    strncmp(map[(int)*s].f[i].fmtvar, var,
					    varlen))
					continue;
				/* fmtvar's resets the format var */
				var = NULL;
				varlen = 0;
			}

			if (tag_add(fconf, &fconf->fmt[fmtid],
				    map[(int)*s].f[i].spec,
				    map[(int)*s].f[i].tag,
				    var ? var : map[(int)*s].f[i].var,
				    var ? varlen : -1,
				    map[(int)*s].f[i].col,
				    map[(int)*s].f[i].parser,
				    map[(int)*s].f[i].tag_flags,
				    errstr, errstr_size) == -1)
				return -1;
		}


		t = ++s;
	}

	/* "..%x....."
	 *      ^---^  add this part as verbatim string */
	if (s > t)
		if (format_add(fconf, 0, NULL, 0,
			       t, (int)(s - t), 0,
			       errstr, errstr_size) == -1)
			return -1;

	/* Dump parsed format string. */
	if (conf.log_level >= 7)
		fmt_dump(fconf);


	if (fconf->fmt_cnt == 0) {
		snprintf(errstr, errstr_size,
			 "%s format string is empty",
			 fmt_conf_names[fconf->fid]);
		return -1;
	} else if (cnt == 0) {
		snprintf(errstr, errstr_size,
			 "No %%.. formatters in %s format",
			 fmt_conf_names[fconf->fid]);
		return -1;
	}

	return fconf->fmt_cnt;
}








/**
 * Kafka outputter
 */
void out_kafka (struct fmt_conf *fconf, struct logline *lp,
		const char *buf, size_t len) {

	/* If 'buf' is the key we simply store it for later use
	 * when the message is produced. */
	if (fconf->fid == FMT_CONF_KEY) {
		assert(!lp->key);
		lp->key = malloc(len);
		lp->key_len = len;
		memcpy(lp->key, buf, len);
		return;
	}

	if (rd_kafka_produce(rkt, conf.partition, RD_KAFKA_MSG_F_COPY,
			     (void *)buf, len,
			     lp->key, lp->key_len, NULL) == -1) {
		vk_log("PRODUCE", LOG_WARNING,
		       "Failed to produce kafka message: %s",
		       strerror(errno));
	}

	rd_kafka_poll(rk, 0);
}


/**
 * Stdout outputter
 */
void out_stdout (struct fmt_conf *fconf, struct logline *lp,
		 const char *buf, size_t len) {
	printf("%.*s\n", (int)len, buf);
}

/**
 * Null outputter
 */
void out_null (struct fmt_conf *fconf, struct logline *lp,
	       const char *buf, size_t len) {
}


/**
 * Currently selected outputter.
 */
void (*outfunc) (struct fmt_conf *fconf, struct logline *lp,
		 const char *buf, size_t len) = out_kafka;


/**
 * Kafka error callback
 */
static void kafka_error_cb (rd_kafka_t *rk, int err,
			    const char *reason, void *opaque) {
	vk_log("KAFKAERR", LOG_ERR, "Kafka error (%i): %s", err, reason);
}


/**
 * Kafka message delivery report callback.
 * Called for each delivered (or failed delivery) message.
 * NOTE: If the dr callback is not to be used it can be turned off to
 *       improve performance.
 */
static void kafka_dr_cb (rd_kafka_t *rk,
			 void *payload, size_t len,
			 int error_code,
			 void *opaque, void *msg_opaque) {
	_DBG("Kafka delivery report: error=%i, size=%zd", error_code, len);
}


static void render_match_string (struct fmt_conf *fconf, struct logline *lp) {
	char buf[4096];
	int  of = 0;
	int  i;

	/* Render each formatter in order. */
	for (i = 0 ; i < fconf->fmt_cnt ; i++) {
		const void *ptr;
		int len = lp->match[fconf->fid][i].len;

		/* Either use accumulated value, or the default value. */
		if (len) {
			ptr = lp->match[fconf->fid][i].ptr;
		} else {
			ptr = fconf->fmt[i].def;
			len = fconf->fmt[i].deflen;
		}

		if (of + len >= sizeof(buf))
			break;

		memcpy(buf+of, ptr, len);
		of += len;
	}

	/* Pass rendered log line to outputter function */
	outfunc(fconf, lp, buf, of);
}


static void render_match_json (struct fmt_conf *fconf, struct logline *lp) {
	yajl_gen g;
	int      i;
	const unsigned char *buf;
#if YAJL_MAJOR < 2
	unsigned int buflen;
#else
	size_t   buflen;
#endif

#if YAJL_MAJOR < 2
	g = yajl_gen_alloc(NULL, NULL);
#else
	g = yajl_gen_alloc(NULL);
#endif
	yajl_gen_map_open(g);

	/* Render each formatter in order. */
	for (i = 0 ; i < fconf->fmt_cnt ; i++) {
		const void *ptr;
		int len = lp->match[fconf->fid][i].len;

		/* Skip constant strings */
		if (fconf->fmt[i].id == 0)
			continue;

		/* Either use accumulated value, or the default value. */
		if (len) {
			ptr = lp->match[fconf->fid][i].ptr;
		} else {
			ptr = fconf->fmt[i].def;
			len = fconf->fmt[i].deflen;
		}

		/* Field name */
		if (likely(fconf->fmt[i].name != NULL))
			yajl_gen_string(g, (unsigned char *)fconf->fmt[i].name,
					fconf->fmt[i].namelen);
		else {
			char name = (char)fconf->fmt[i].id;
			yajl_gen_string(g, (unsigned char *)&name, 1);
		}

		/* Value */
		switch (fconf->fmt[i].type)
		{
		case FMT_TYPE_STRING:
			yajl_gen_string(g, ptr, len);
			break;
		case FMT_TYPE_NUMBER:
			yajl_gen_number(g, ptr, len);
			break;
		}
	}

	yajl_gen_map_close(g);

	yajl_gen_get_buf(g, &buf, &buflen);

	/* Pass rendered log line to outputter function */
	outfunc(fconf, lp, (const char *)buf, buflen);

	yajl_gen_clear(g);
	yajl_gen_free(g);
}

/**
 * Render an accumulated logline to string and pass it to the output function.
 */
static void render_match (struct logline *lp, uint64_t seq) {
	int i;

	lp->seq = seq;

	/* Render fmt_confs in reverse order so KEY is available for MAIN */
	for (i = conf.fconf_cnt-1 ; i >= 0 ; i--) {
		struct fmt_conf *fconf = &conf.fconf[i];
		switch (fconf->encoding)
		{
		case VK_ENC_STRING:
			render_match_string(fconf, lp);
			break;
		case VK_ENC_JSON:
			render_match_json(fconf, lp);
			break;
		}
	}
}


/**
 * Resets the given logline and makes it ready for accumulating a new request.
 */
static void logline_reset (struct logline *lp) {
	int i;

	/* Clear logline, except for scratch pad since it will be overwritten */

	for (i = 0 ; i < conf.fconf_cnt ; i++)
		memset(lp->match[i], 0,
		       conf.fconf[i].fmt_cnt * sizeof(*lp->match[i]));
	
	if (lp->key) {
		free(lp->key);
		lp->key = NULL;
		lp->key_len = 0;
	}

	lp->seq       = 0;
	lp->sof       = 0;
	lp->tags_seen = 0;
}


/**
 * Returns a logline.
 */
static inline struct logline *logline_get (unsigned int id) {
	struct logline *lp;

	if (unlikely(id >= logline_cnt)) {
		int newcnt = id + 64;

		_DBG("Reallocate logline array "
		     "from %i to %i entries (new id %i)",
		     logline_cnt, newcnt, id);
		     
		loglines = realloc(loglines, sizeof(*loglines) * newcnt);
		if (!loglines) {
			vk_log("MEM", LOG_CRIT,
			       "Unable to allocate new logline "
			       "array of size %lu bytes (%i entries): %s",
			       sizeof(*loglines) * newcnt, newcnt,
			       strerror(errno));
			conf.run = 0;
			conf.pret = -1;
			return NULL;
		}

		memset(loglines + logline_cnt, 0,
		       sizeof(*loglines) * (newcnt - logline_cnt));
		
		logline_cnt = newcnt;
	}

	if (unlikely(!(lp = loglines[id]))) {
		int i;
		char *ptr;

		/* Allocate a new logline if necessary. */
		lp = loglines[id] = calloc(1, sizeof(*lp) + 
					   (conf.total_fmt_cnt *
					    sizeof(*lp->match[0])));
		ptr = (char *)(lp+1);
		for (i = 0 ; i < conf.fconf_cnt ; i++) {
			lp->match[i] = (struct match *)ptr;
			ptr += conf.fconf[i].fmt_cnt * sizeof(*lp->match[i]);
		}
	}

	return lp;
}





/**
 * Given a single tag 'tagid' with its data 'ptr' and 'len';
 * try to match it to the registered format tags.
 *
 * Returns 1 if the line is done and can be rendered, else 0.
 */
static int tag_match (struct logline *lp, int spec, enum VSL_tag_e tagid,
		      const char *ptr, int len) {
	const struct tag *tag;

	/* Iterate through all handlers for this tag. */
	for (tag = conf.tag[tagid] ; tag ; tag = tag->next) {
		const char *ptr2;
		int len2;

		/* Value already assigned */
		if (lp->match[tag->fid][tag->fmt->idx].ptr)
			continue;

		/* Match spec (client or backend) */
		if (!(tag->spec & spec))
			continue;

		if (tag->var && !(tag->flags & TAG_F_NOVARMATCH)) {
			const char *t;
			
			/* Variable match ("Varname: value") */
			if (!(t = strnchr(ptr, len, ':')))
				continue;
			
			if (tag->varlen != (int)(t-ptr) ||
			    strncasecmp(ptr, tag->var, tag->varlen))
				continue;

			if (likely(len > tag->varlen + 1 /* ":" */)) {
				ptr2 = t+2; /* ": " */
				len2 = len - (int)(ptr2-ptr);
			} else {
				/* Empty value */
				len2 = 0;
				ptr2 = NULL;
			}

		} else {
			ptr2 = ptr;
			len2 = len;
		}

		/* Get specified column if specified. */
		if (tag->col)
			if (!column_get(tag->col, ' ', ptr2, len2,
					&ptr2, &len2) == -1)
				continue;

		if (tag->parser) {
			/* Pass value to parser which will assign it. */
			tag->parser(tag, lp, ptr2, len2);
			
		} else {
			/* Fallback to verbatim field. */
			match_assign(tag, lp, ptr2, len2);
		}

	}

	/* Request end: render the match string. */
	if (tagid == SLT_ReqEnd)
		return 1;
	else
		return 0;
}


/**
 * VSL_Dispatch() callback called for each tag read from the VSL.
 */
static int parse_tag (void *priv, enum VSL_tag_e tag, unsigned id,
		      unsigned len, unsigned spec, const char *ptr,
		      uint64_t bitmap) {
	struct logline *lp;
	int    is_complete = 0;

	if (unlikely(!spec))
		return conf.pret;

	if (0)
		_DBG("[%u] #%-3i %-12s %c %.*s",
		     id, tag, VSL_tags[tag],
		     spec & VSL_S_CLIENT ? 'c' : 'b',
		     len, ptr);

	if (unlikely(!(lp = logline_get(id))))
		return -1;

	/* Update bitfield of seen tags */
	lp->tags_seen |= bitmap;

	/* Accumulate matched tag content */
	if (likely(!(is_complete = tag_match(lp, spec, tag, ptr, len))))
		return conf.pret;

	/* Match tag regexp, if any */
	if (conf.m_flag && !VSL_Matched(vd, lp->tags_seen)) {
		logline_reset(lp);
		return conf.pret;
	}
	
	/* Log line is complete: render & output */
	render_match(lp, ++conf.sequence_number);

	/* clean up */
	logline_reset(lp);

	return conf.pret;
}


/**
 * varnishkafka logger
 */
void vk_log0 (const char *func, const char *file, int line,
	      const char *facility, int level, const char *fmt, ...) {
	va_list ap;
	char buf[512];

	if (level > conf.log_level || !conf.log_to)
		return;

	va_start(ap, fmt);
	vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);

	if (conf.log_to & VK_LOG_SYSLOG)
		syslog(level, "%s: %s", facility, buf);

	if (conf.log_to & VK_LOG_STDERR)
		fprintf(stderr, "%%%i %s: %s\n", level, facility, buf);
}


/**
 * Termination signal handler.
 * May be called multiple times (multiple SIGTERM/SIGINT) since a
 * blocking VSL_Dispatch() call cannot be aborted:
 *  - first signal: flag to exit VSL_Dispatch() when the next tag is read.
 *                  if succesful the kafka producer can send remaining messages
 *  - second signal: exit directly, queued kafka messages will be lost.
 *
 */
static void sig_term (int sig) {
	vk_log("TERM", LOG_NOTICE,
	       "Received signal %i: terminating", sig);
	conf.pret = -1;
	if (--conf.run <= -1) {
		vk_log("TERM", LOG_WARNING, "Forced termination");
		exit(0);
	}
}


static void usage (const char *argv0) {
	fprintf(stderr,
		"varnishkafka version %s\n"
		"Varnish log listener with Apache Kafka producer support\n"
		"\n"
		"Usage: %s [VSL_ARGS] [-S <config-file>]\n"
		"\n"
		" VSL_ARGS are standard Varnish VSL arguments:\n"
		"  %s\n"
		"\n"
		" The VSL_ARGS can also be set through the configuration file\n"
		" with \"varnish.arg.<..> = <..>\"\n"
		"\n"
		" Default configuration file path: %s\n"
		"\n",
		VARNISHKAFKA_VERSION,
		argv0,
		VSL_USAGE,
		VARNISHKAFKA_CONF_PATH);
	exit(1);
}


int main (int argc, char **argv) {
	char errstr[512];
	char hostname[1024];
	struct hostent *lh;
	char c;
	int r;
	int i;

	/*
	 * Default configuration
	 */
	conf.log_level = 6;
	conf.log_to    = VK_LOG_STDERR;
	conf.daemonize = 1;
	conf.datacopy  = 1;
	conf.rk_conf = rd_kafka_conf_new();
	rd_kafka_conf_set(conf.rk_conf, "client.id", "varnishkafka", NULL, 0);
	rd_kafka_conf_set_error_cb(conf.rk_conf, kafka_error_cb);
	rd_kafka_conf_set_dr_cb(conf.rk_conf, kafka_dr_cb);
	rd_kafka_conf_set(conf.rk_conf, "queue.buffering.max.messages",
			  "1000000", NULL, 0);

	conf.topic_conf = rd_kafka_topic_conf_new();
	rd_kafka_topic_conf_set(conf.topic_conf, "required_acks", "1", NULL, 0);

	for (i = 0 ; i < FMT_CONF_NUM ; i++)
		conf.fconf[i].fid = i;
		
	conf.format[FMT_CONF_MAIN] = "%l %n %t %{Varnish:time_firstbyte}x %h "
		"%{Varnish:handling}x/%s %b %m http://%{Host}i%U%q - - "
		"%{Referer}i %{X-Forwarded-For}i %{User-agent}i";

	/* Construct logname (%l) from local hostname */
	gethostname(hostname, sizeof(hostname)-1);
	hostname[sizeof(hostname)-1] = '\0';
	lh = gethostbyname(hostname);
	conf.logname = strdup(lh->h_name);


	/* Create varnish shared memory handle */
	vd = VSM_New();
	VSL_Setup(vd);

	/* Parse command line arguments */
	while ((c = getopt(argc, argv, VSL_ARGS "hS:")) != -1) {
		switch (c) {
		case 'h':
			usage(argv[0]);
			break;
		case 'S':
			conf_file_path = optarg;
			break;
		case 'm':
			conf.m_flag = 1;
			/* FALLTHRU */
		default:
			if ((r = VSL_Arg(vd, c, optarg)) == 0)
				usage(argv[0]);
			else if (r == -1)
				exit(1); /* VSL_Arg prints error message */
			break;
		}
	}

	/* Read config file */
	if (conf_file_read(conf_file_path) == -1)
		exit(1);

	if (!conf.topic)
		usage(argv[0]);

	/* Always include client communication (-c) */
	VSL_Arg(vd, 'c', NULL);

	/* Set up syslog */
	if (conf.log_to & VK_LOG_SYSLOG)
		openlog("varnishkafka", LOG_PID|LOG_NDELAY, LOG_DAEMON);

	/* Termination signal handlers */
	signal(SIGINT, sig_term);
	signal(SIGTERM, sig_term);

	/* Ignore network disconnect signals, handled by rdkafka */
	signal(SIGPIPE, SIG_IGN);

	/* Initialize base64 decoder */
	VB64_init();

	/* Space is the most common format separator so add it first
	 * the the const string, followed by the typical default value "-". */
	const_string_add(" -", 2);

	/* Allocate room for format tag buckets. */
	conf.tag = calloc(VSL_TAGS_MAX, sizeof(*conf.tag));

	/* Parse the format strings */
	for (i = 0 ; i < FMT_CONF_NUM ; i++) {
		if (!conf.format[i])
			continue;

		if (format_parse(&conf.fconf[i], conf.format[i],
				 errstr, sizeof(errstr)) == -1) {
			vk_log("FMTPARSE", LOG_ERR,
			       "Failed to parse %s format string: %s\n%s",
			       fmt_conf_names[i], conf.format[i], errstr);
			exit(1);
		}

		conf.fconf_cnt++;
		conf.total_fmt_cnt += conf.fconf[i].fmt_cnt;
	}

	if (conf.fconf_cnt == 0) {
		vk_log("FMT", LOG_ERR, "No formats defined");
		exit(1);
	}

	if (conf.log_level >= 7)
		tag_dump();

	/* Open the log file */
	if (VSL_Open(vd, 1) != 0) {
		vk_log("VSLOPEN", LOG_ERR, "Failed to open Varnish VSL: %s\n",
		       strerror(errno));
		exit(1);
	}

	/* Daemonize if desired */
	if (conf.daemonize) {
		if (daemon(0, 0) == -1) {
			vk_log("KAFKANEW", LOG_ERR, "Failed to daemonize: %s",
			       strerror(errno));
			exit(1);
		}
		conf.log_to &= ~VK_LOG_STDERR;
	}

	/* Kafka outputter */
	if (outfunc == out_kafka) {
		/* Create Kafka handle */
		if (!(rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf.rk_conf,
					errstr, sizeof(errstr)))) {
			vk_log("KAFKANEW", LOG_ERR,
			       "Failed to create kafka handle: %s", errstr);
			exit(1);
		}

		rd_kafka_set_log_level(rk, conf.log_level);

		/* Create Kafka topic handle */
		if (!(rkt = rd_kafka_topic_new(rk, conf.topic,
					       conf.topic_conf))) {
			vk_log("KAFKANEW", LOG_ERR,
			       "Invalid topic or configuration: %s: %s",
			       conf.topic, strerror(errno));
			exit(1);
		}
	}

	/* Main dispatcher loop depending on outputter */
	conf.run = 1;
	conf.pret = 0;

	if (outfunc == out_kafka) {
		/* Kafka outputter */

		while (conf.run && VSL_Dispatch(vd, parse_tag, NULL) >= 0)
			rd_kafka_poll(rk, 0);

		/* Run until all kafka messages have been delivered
		 * or we are stopped again */
		conf.run = 1;

		while (conf.run && rd_kafka_outq_len(rk) > 0)
			rd_kafka_poll(rk, 100);

		rd_kafka_destroy(rk);

	} else {
		/* Stdout outputter */

		while (conf.run && VSL_Dispatch(vd, parse_tag, NULL) >= 0)
			;

	}

	VSM_Close(vd);
	exit(0);
}
