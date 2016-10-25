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
#include <limits.h>
#include <stdbool.h>

#include <vapi/vsm.h>
#include <vapi/vsl.h>
#include <vapi/voptget.h>
#include <vdef.h>

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

static const char *conf_file_path = VARNISHKAFKA_CONF_PATH;

static void logrotate(void);

/**
 * Counters
 */
static struct {
	uint64_t tx;               /* Printed/Transmitted lines */
	uint64_t txerr;            /* Transmit failures */
	uint64_t kafka_drerr;      /* Kafka: message delivery errors */
	uint64_t trunc;            /* Truncated tags */
} cnt;

static void print_stats (void) {
	vk_log_stats("{ \"varnishkafka\": { "
	       "\"time\":%llu, "
	       "\"tx\":%"PRIu64", "
	       "\"txerr\":%"PRIu64", "
	       "\"kafka_drerr\":%"PRIu64", "
	       "\"trunc\":%"PRIu64", "
	       "\"seq\":%"PRIu64" "
	       "} }\n",
	       (unsigned long long)time(NULL),
	       cnt.tx,
	       cnt.txerr,
	       cnt.kafka_drerr,
	       cnt.trunc,
	       conf.sequence_number);
}



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
static char *const_string_add (const char *in, size_t inlen) {
	char *ret;
	const char *instr = strndupa(in, inlen);

	if (!const_string || !(ret = strstr(const_string, instr))) {
		if (const_string_len + inlen + 1 >= const_string_size) {
			/* Reallocate buffer to fit new string (and more) */
			const_string_size = (const_string_size + inlen + 64)*2;
			const_string = realloc(const_string, const_string_size);
			assert(const_string);
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
static UNUSED void fmt_dump (void) {
	int i;

	_DBG("Main %i/%i formats:",
	     fconf.fmt_cnt, fconf.fmt_size);
	for (i = 0 ; i < fconf.fmt_cnt ; i++) {
		struct fmt *fmt = &fconf.fmt[i];
		_DBG(" #%-3i  fmt %i (%c)  var \"%s\", def (%zu)\"%.*s\"%s",
		     i,
		     fmt->id,
		     isprint(fmt->id) ? (char)fmt->id : ' ',
		     fmt->var ? : "",
		     fmt->deflen, (int)fmt->deflen,
		     fmt->def,
		     fmt->flags & FMT_F_ESCAPE ? ", escape" : "");
	}
}

/**
 * Print parser format string: tags
 */
static UNUSED void tag_dump (void) {
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
static int format_add (int fmtr, const char *var, ssize_t varlen,
		       const char *def, ssize_t deflen,
		       int flags) {
	struct fmt *fmt;

	if (fconf.fmt_cnt >= fconf.fmt_size) {
		fconf.fmt_size = (fconf.fmt_size ? : 32) * 2;
		fconf.fmt = realloc(fconf.fmt,
				     fconf.fmt_size * sizeof(*fconf.fmt));
	}

	fmt = &fconf.fmt[fconf.fmt_cnt];
	memset(fmt, 0, sizeof(*fmt));

	fmt->id    = fmtr;
	fmt->idx   = fconf.fmt_cnt;
	fmt->flags = flags;
	if (var) {
		if (varlen == -1)
			varlen = strlen(var);
		char* fvar = malloc(varlen+1);
		memcpy(fvar, var, varlen);
		fvar[varlen] = '\0';
		fmt->var = fvar;
	} else {
		fmt->var = NULL;
	}

	if (!def)
		def = "-";

	if (deflen == -1)
		deflen = strlen(def);
	fmt->deflen = deflen;
	fmt->def = const_string_add(def, deflen);

	fconf.fmt_cnt++;

	return fmt->idx;
}



/**
 * Adds a parsed tag to the list of tags
 */
static int tag_add (struct fmt *fmt, int spec, int tagid,
		    const char *var, ssize_t varlen, int col,
		    size_t (*parser) (const struct tag *tag, struct logline *lp,
				   const char *ptr, size_t len),
		    int tag_flags) {
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

	if (var) {
		if (varlen == -1)
			varlen = strlen(var);

		tag->var = malloc(varlen+1);
		tag->varlen = varlen;
		memcpy(tag->var, var, varlen);
		tag->var[varlen] = '\0';
	} else {
		tag->var = NULL;
	}

	return 0;
}


/**
 * Allocate persistent memory space ('len' bytes) in lp scratch
 */
static char *scratch_alloc (struct logline *lp, size_t len) {
	char *ptr;

	if (unlikely(len > conf.scratch_size || (conf.scratch_size - len) < lp->sof)) {
		vk_log("SCRATCH", LOG_CRIT, "Ran out of scratch_size, limit is %zu", conf.scratch_size);
		exit(99);
	}

	ptr = lp->scratch + lp->sof;
	lp->sof += len;
	return ptr;
}


/**
 * essentially, scratch_alloc + memcpy
 */
static char *scratch_cpy (struct logline *lp, const char *src, size_t len) {
	char *dst;

	dst = scratch_alloc(lp, len);
	memcpy(dst, src, len);
	return dst;
}


/**
 * like scratch_cpy, but escapes all unprintable characters as well as the
 * ones defined in 'map' below.  sets *len to the escaped length of the result
 */
static char* scratch_cpy_esc (struct logline *lp, const char *src, size_t* len) {
	static const char *map[256] = {
		['\t'] = "\\t",
		['\n'] = "\\n",
		['\r'] = "\\r",
		['\v'] = "\\v",
		['\f'] = "\\f",
		['"']  = "\\\"",
		[' ']  = "\\ ",
	};


	/* Allocate initial space for escaped string.
	 * The maximum expansion size per character is 5 (octal coding).
	 */
	const size_t in_len = *len;
	char dst[in_len * 5];
	char *d = dst;
	const char *s = src;
	const char *srcend = src + in_len;

	while (s < srcend) {
		size_t outlen = 1;
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

		assert(outlen < (in_len * 5));

		if (likely(outlen == 1)) {
			*(d++) = *out;
		} else {
			memcpy(d, out, outlen);
			d += outlen;
		}

		s++;
	}

	assert(d > dst);
	const size_t out_len = d - dst;
	*len = out_len;
	return scratch_cpy(lp, dst, out_len);
}


/**
 * sprintf into a new scratch allocation.  *len_out will be set to the length
 * of the result in the scratch.
 */
__attribute__((format(printf,3,4)))
static char* scratch_printf (struct logline *lp, size_t* len_out, const char *fmt, ...) {
	va_list ap, ap2;
	int r;

	va_copy(ap2, ap);
	va_start(ap2, fmt);
	r = vsnprintf(NULL, 0, fmt, ap2);
	va_end(ap2);

	assert(r > 0);
	size_t rst = (size_t)r;

	*len_out = rst;
	char *dst = scratch_alloc(lp, rst + 1);

	va_start(ap, fmt);
	vsnprintf(dst, r+1, fmt, ap);
	va_end(ap);

	return dst;
}

/**
 * raw assign to lp->match, only used by match_assign() below
 */
static void match_assign0 (const struct tag *tag, struct logline *lp,
				  const char *ptr, size_t len) {
	lp->match[tag->fmt->idx].ptr = ptr;
	lp->match[tag->fmt->idx].len = len;
}


/**
 * Assign 'ptr' of size 'len' as a match for 'tag' in logline 'lp'.
 *
 * if 'ptr' is non-persistent (e.g. stack allocation, as opposed to original
 * VSL shm tag payload), you must set force_copy to 'true'
 */
static void match_assign (const struct tag *tag, struct logline *lp,
			  const char *ptr, size_t len, bool force_copy) {

	if (unlikely(tag->fmt->flags & FMT_F_ESCAPE)) {
		size_t len_io = len;
		char* escaped = scratch_cpy_esc(lp, ptr, &len_io);
		match_assign0(tag, lp, escaped, len_io);
	} else {
		if (force_copy) /* copy volatile data */
			match_assign0(tag, lp, scratch_cpy(lp, ptr, len), len);
		else  /* point to persistent data */
			match_assign0(tag, lp, ptr, len);
	}
}


static char *strnchr_noconst (char *s, size_t len, int c) {
	char *end = s + len;
	while (s < end) {
		if (*s == c)
			return s;
		s++;
	}

	return NULL;
}

static const char *strnchr (const char *s, size_t len, int c) {
	const char *end = s + len;
	while (s < end) {
		if (*s == c)
			return s;
		s++;
	}

	return NULL;
}


/**
 * Looks for any matching character from 'match' in 's' and returns
 * a pointer to the first match, or NULL if none of 'match' matched 's'.
 */
static const char *strnchrs (const char *s, size_t len, const char *match) {
	const char *end = s + len;
	char map[256] = {};
	while (*match)
		map[(int)*(match++)] = 1;

	while (s < end) {
		if (map[(int)*s])
			return s;
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
static int column_get (int col, char delim, const char *ptr, size_t len,
		       const char **dst, size_t *dstlen) {
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
			*dstlen = (s - b);
			return 1;
		}

		b = ++s;
	}

	if (s != b && col == ++i) {
		*dst = b;
		*dstlen = (s - b);
		return 1;
	}

	return 0;
}



/**
 * Misc parsers for formatters
 * (check format_parse() for more info)
 */

/**
 * Parse a URL (without query string) retrieved from a tag's payload.
 */
static size_t parse_U (const struct tag *tag, struct logline *lp,
		    const char *ptr, size_t len) {
	const char *qs;
	size_t slen = len;

	// Remove the query string if present
	if ((qs = strnchr(ptr, len, '?')))
		slen = (qs - ptr);

	match_assign(tag, lp, ptr, slen, false);
	return slen;
}

/**
 * Parse a query-string retrieved from a tag's payload.
 */
static size_t parse_q (const struct tag *tag, struct logline *lp,
		    const char *ptr, size_t len) {
	const char *qs;
	size_t slen = len;

	if (!(qs = strnchr(ptr, len, '?')))
		return 0;

	slen = len - (qs - ptr);

	match_assign(tag, lp, qs, slen, false);
	return slen;
}

/**
 * Parse a timestamp retrieved from a tag's payload.
 */
static size_t parse_t (const struct tag *tag, struct logline *lp,
		    const char *ptr, size_t len) {
	struct tm tm;
	const char *timefmt = "[%d/%b/%Y:%T %z]";
	const int timelen   = 64;
	size_t tlen;

	/*
	 * The special format for tag->var "end:strftime" is used
	 * to force Varnishkafka to use the SLT_Timestamp 'Resp' instead
	 * of 'Start' for timestamp formatters. The prefix is removed
	 * from 'timefmt' accordingly.
	 */
	if (tag->var){
		const char *fmt_tmp = tag->var;
		// Remove APACHE_LOG_END_PREFIX from the format string
		if (tag->flags & TAG_F_TIMESTAMP_END) {
			fmt_tmp += strlen(APACHE_LOG_END_PREFIX);
		}
		/* If the rest of the format string without the
		 * 'end:' prefix is not empty, use it
		 * in place of the default.
		 */
		if (*fmt_tmp)
			timefmt = fmt_tmp;
	}

	time_t t = strtoul(ptr, NULL, 10);
	localtime_r(&t, &tm);

	char dst[timelen];

	/* Format time string */
	tlen = strftime(dst, timelen, timefmt, &tm);

	match_assign(tag, lp, dst, tlen, true);
	return tlen;
}

static size_t parse_auth_user (const struct tag *tag, struct logline *lp,
			    const char *ptr, size_t len) {
	size_t rlen = len - 6/*"basic "*/;
	size_t ulen;
	char *q;

	if (unlikely(rlen == 0 || strncasecmp(ptr, "basic ", 6) || (rlen % 2)))
		return 0;

	/* Calculate base64 decoded length */
	if (unlikely(!(ulen = (rlen * 4) / 3)))
		return 0;

	/* Protect our stack */
	if (unlikely(ulen > 1000))
		return 0;

	char tmp[ulen + 1];

	if ((ulen = VB64_decode2(tmp, ulen, ptr+6, rlen)) <= 0)
		return 0;

	/* Strip password */
	if ((q = strnchr_noconst(tmp, ulen, ':')))
		*q = '\0';

	const size_t out_len = strlen(tmp);
	match_assign(tag, lp, tmp, out_len, true);
	return out_len;
}


/* The VCL_call is used for several info; this function matches the only
 * ones that varnishkafka cares about and discards the other ones.
 */
static size_t parse_vcl_handling (const struct tag *tag, struct logline *lp,
			   const char *ptr, size_t len) {
	if ((len == 3 && !strncmp(ptr, "HIT", 3)) ||
	    (len == 4 && (!strncmp(ptr, "MISS", 4) ||
			  !strncmp(ptr, "PASS", 4)))) {
		match_assign(tag, lp, ptr, len, false);
		return len;
	}
	return 0;
}

static size_t parse_seq (const struct tag *tag, struct logline *lp,
		      const char *ptr UNUSED, size_t len UNUSED) {
	size_t len_out = 0;
	char* out = scratch_printf(lp, &len_out, "%"PRIu64, conf.sequence_number);
	match_assign(tag, lp, out, len_out, false);
	return len_out;
}

static size_t parse_DT (const struct tag *tag, struct logline *lp,
		     const char *ptr, size_t len) {

	/* SLT_Timestamp logs timing info in ms */
	double time_taken_ms;

	/* ptr points to the original tag string, so we
	 * need to extract the double field needed
	 */
	if (!(time_taken_ms = atof(strndupa(ptr, len))))
		return 0;

	size_t len_out = 0;

	if (tag->fmt->id == (int)'D') {
		char* out = scratch_printf(lp, &len_out, "%.0f", time_taken_ms * 1000000.0f);
		match_assign(tag, lp, out, len_out, false);
	} else if (tag->fmt->id == (int)'T') {
		char* out = scratch_printf(lp, &len_out, "%f", time_taken_ms);
		match_assign(tag, lp, out, len_out, false);
	}

	return len_out;
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
	assert(out);

	s = sp = in;
	while (*s) {
		const char **a;

		for (a = arr ; *a ; a += 2) {
			const char *from = a[0];
			const char *to   = a[1];
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

			if (of + tolen >= outsize) {
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
		if ( of + (int)(s-sp) >= outsize) {
			outsize = ( of + (int)(s-sp) + 1 );
			out = realloc(out, outsize);
			assert(out);
		}
		memcpy(out+of, sp, (int)(s-sp));
		of += (int)(s-sp);
	}

	out[of] = '\0';

	return out;
}


/**
 * Parse the format string and build a parsing array.
 */
static int format_parse (const char *format_orig,
			 char *errstr, size_t errstr_size) {
	/**
	 * Maps a formatter %X to a VSL tag and column id, or parser, or both
	 */
	struct {
		/* A formatter may be backed by multiple tags.
		 * The first matching tag observed in the log will be used.
		 *
		 * Example:
		 * A formatter declared as following in 'map':
		 * ['x'] = { {
		 *		{ VSL_CLIENTMARKER, SLT_Timestamp,
		 *		  .var = "Process",
		 *		  .fmtvar = "Varnish:time_firstbyte", .col = 2 },
		 *		{ VSL_CLIENTMARKER, SLT_Begin,
		 *		  .fmtvar = "Varnish:xvid", .col = 2 }
		 *	} },
		 *
		 * In this case, the struct below is replicated two
		 * times to match the 'x' formatter to multiple
		 * Varnish tags (establishing also a priority).
		 * Therefore, supposing that 'x' corresponds to 'map[42]',
		 * the following variables will be accessible:
		 * - map[42].f[0].var    ==> "Process"
		 * - map[42].f[1].fmtvar ==> "Varnish:xvid"
		 */
		struct {
			/* VSL_CLIENTMARKER or VSL_BACKENDMARKER, or both */
			int spec;
			/* The SLT_.. tag id */
			int tag;
			/* For "Name: Value" tags (such as SLT_RespHeader),
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
			size_t (*parser) (const struct tag *tag,
				       struct logline *lp,
				       const char *ptr, size_t len);
			/* Optional tag->flags */
			int tag_flags;
		} f[5+1]; /* increase size when necessary (max used size + 1) */

		/* Default string if no matching tag was found or all
		 * parsers failed, defaults to "-". */
		const char *def;

	} map[256] = {
		/* Indexed by formatter character as
		 * specified by varnishncsa(1).
		 * Each formatter is associated with
		 * the structure defined above; please
		 * note that not all of the fields are mandatory!
		 *
		 * Important note: you can see the next
		 * configurations as pipes. For example,
		 * setting "SLT_Y, var: X, col:2, parser:foo
		 * will allow you to match something with a
		 * Varnish tag named SLT_Y, carrying a payload
		 * like "X: a b c d e" selecting the second field
		 * and passing it to a parser function
		 * (for perf reason the field passed is pointer to
		 * the start of the substring in the original payload
		 * plus its length, keep it in mind when writing a parser).
		 */
		['b'] = { {
				/* Size of response in bytes, with HTTP headers. */
				{ VSL_CLIENTMARKER, SLT_ReqAcct, .col = 5}
			} },
		['D'] = { {
				/* Time taken to serve the request (s) */
				{ VSL_CLIENTMARKER, SLT_Timestamp,
				  .var = "Resp", .col = 2,
				  .parser = parse_DT}
			} },
		['T'] = { {
				/* Time taken to serve the request (s) */
				{ VSL_CLIENTMARKER, SLT_Timestamp,
				  .var = "Resp", .col = 2,
				  .parser = parse_DT}
			} },
		['H'] = { {
				/* The request protocol */
				{ VSL_CLIENTMARKER, SLT_ReqProtocol },
			}, .def = "HTTP/1.0" },
		['h'] = { {
				/* Remote hostname (IP address) */
				{ VSL_CLIENTMARKER, SLT_ReqStart, .col = 1},
			} },
		['i'] = { {
				/* Used as %{VARNAME}i.
				 * The contents of VARNAME: header line(s)
				 * in the request sent to the server.
				 */
				{ VSL_CLIENTMARKER, SLT_ReqHeader,
				  .tag_flags = TAG_F_LAST }
			} },
		['l'] = { {
				{ VSL_CLIENTMARKER }
			}, .def = conf.logname },
		['m'] = { {
				/* Request method (GET|POST|..) */
				{ VSL_CLIENTMARKER, SLT_ReqMethod }
			} },
		['q'] = { {
				/* The request query string */
				{ VSL_CLIENTMARKER, SLT_ReqURL, .parser = parse_q }
			},  .def = "" },
		['o'] = { {
				/* Used as %{VARNAME}o.
				 * The contents of VARNAME: header line(s)
				 * in the response sent to the server.
				 */
				{ VSL_CLIENTMARKER, SLT_RespHeader,
				  .tag_flags = TAG_F_LAST }
			} },
		['s'] = { {
				/* The response HTTP status */
				{ VSL_CLIENTMARKER, SLT_RespStatus,
				  .tag_flags = TAG_F_LAST }
			} },
		['t'] = { {
				{ VSL_CLIENTMARKER, SLT_Timestamp,
				  .col = 2,
				  .parser = parse_t,
				  .tag_flags = TAG_F_TIMESTAMP }
			} },
		['U'] = { {
				/* The URL path requested, not including any query string. */
				{ VSL_CLIENTMARKER, SLT_ReqURL, .parser = parse_U }
			} },
		['u'] = { {
				{ VSL_CLIENTMARKER, SLT_ReqHeader,
				  .var = "authorization",
				  .parser = parse_auth_user }
			} },
		['x'] = { {
				/* Various Varnish related tags */
				{ VSL_CLIENTMARKER, SLT_Timestamp,
				  .var = "Process",
				  .fmtvar = "Varnish:time_firstbyte", .col = 2 },
				{ VSL_CLIENTMARKER, SLT_Begin,
				  .fmtvar = "Varnish:xvid", .col = 2 },
				{ VSL_CLIENTMARKER, SLT_VCL_call,
				  .fmtvar = "Varnish:handling",
				  .parser = parse_vcl_handling },
				{ VSL_CLIENTMARKER, SLT_VCL_Log,
				  .fmtvar = "VCL_Log:*" },
				{ VSL_CLIENTMARKER, SLT_VSL,
				  .fmtvar = "VSL_API:*",
				  .tag_flags = TAG_F_MATCH_PREFIX },
			} },
		['n'] = { {
				{ VSL_CLIENTMARKER, VSL_TAG__ONCE,
				  .parser = parse_seq }
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
	int counter = 0;

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
		if (s > t) {
			if (format_add(0, NULL, 0, t, (int)(s - t), 0) == -1)
				return -1;
		}

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
							   "@?!"))) {
						qlen = (int)(q2-q);
					} else {
						qlen = (int)(b-q);
					}

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
								 qlen)) {
							flags |= FMT_F_ESCAPE;
						} else if (!strncasecmp(q, "num",
								      qlen)) {
							type = FMT_TYPE_NUMBER;
						} else {
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
			} else {
				varlen = (int)(b-a);
			}

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
		if ((fmtid = format_add(*s, var, varlen,
					def, deflen, flags)) == -1)
			return -1;

		fconf.fmt[fmtid].type = type;

		if (name) {
			fconf.fmt[fmtid].name = name;
			fconf.fmt[fmtid].namelen = namelen;
		}

		counter++;

		/* Now add the matched tags specification to the
		 * list of parse tags */
		for (i = 0 ; map[(int)*s].f[i].spec ; i++) {
			if (map[(int)*s].f[i].tag == 0)
				continue;

			/* The %{format}t formatter handles SLT_Timestamp tags.
			 * Its format allows the use of a prefix like "end:"
			 * to specify what SLT_Timestamp tag to pick:
			 * - "end:" corresponds to Varnish "Resp" timestamp.
			 * - Anything else will default to Varnish "Start" timestamp.
			 * This check is useful to find string prefixes in all
			 * the %{}t timestamp formatters and store the result among its tags
			 * to avoid repeating the same operation at run time for each
			 * request (wasting resources).
			 */
			if ((map[(int)*s].f[i].tag_flags & TAG_F_TIMESTAMP) && var) {
				if (strncmp(var, APACHE_LOG_END_PREFIX,
						strlen(APACHE_LOG_END_PREFIX)) == 0) {
					map[(int)*s].f[i].tag_flags |= TAG_F_TIMESTAMP_END;
				}
			}

			/* mapping has fmtvar specified, make sure it
			 * matches the format's variable. */
			if (map[(int)*s].f[i].fmtvar) {
				const char *iswc;

				if (!var)
					continue;

				/* Match "xxxx:<key>".
				 * If format definition's key is wildcarded
				 * ("*") then use the configured key as var.
				 * I.e., "%{VCL_Log:hit}x" will put "hit" in
				 * var since VCL_Log is defined as wildcard:
				 * "VCL_Log:*". */
				if ((iswc = strstr(map[(int)*s].f[i].fmtvar,
						   ":*"))) {
					/* Wildcard definition */
					int fvlen = (int)(iswc -
							  map[(int)*s].
							  f[i].fmtvar) + 1;

					/* Check that var matches prefix.
					 * I.e.: "VCL_Log:" cmp "VCL_Log:" */
					if (varlen <= fvlen ||
					    strncmp(map[(int)*s].f[i].fmtvar,
						    var, fvlen))
						continue;

					/* set format var to "..:<key>" */
					var = var + fvlen;
					varlen -= fvlen;
				} else {
					/* Non-wildcard definition.
					 * Var must match exactly. */
					if (varlen != strlen(map[(int)*s].
							     f[i].fmtvar) ||
					    strncmp(map[(int)*s].f[i].fmtvar,
						    var, varlen))
						continue;

					/* fmtvar's resets the format var */
					var = NULL;
					varlen = 0;
				}
			}

			if (tag_add(&fconf.fmt[fmtid],
				    map[(int)*s].f[i].spec,
				    map[(int)*s].f[i].tag,
				    var ? var : map[(int)*s].f[i].var,
				    var ? varlen : -1,
				    map[(int)*s].f[i].col,
				    map[(int)*s].f[i].parser,
				    map[(int)*s].f[i].tag_flags
				   ) == -1)
				return -1;
		}

		t = ++s;
	}

	/* "..%x....."
	 *      ^---^  add this part as verbatim string */
	if (s > t) {
		if (format_add(0, NULL, 0, t, (int)(s - t), 0) == -1)
			return -1;
	}

	/* Dump parsed format string. */
	if (conf.log_level >= 7)
		fmt_dump();

	if (fconf.fmt_cnt == 0) {
		snprintf(errstr, errstr_size,
			 "Main format string is empty");
		return -1;
	} else if (counter == 0) {
		snprintf(errstr, errstr_size,
			 "No %%.. formatters in Main format");
		return -1;
	}

	return fconf.fmt_cnt;
}



/**
 * Simple rate limiter used to limit the amount of error syslogs.
 * Controlled through the "log.rate.max" and "log.rate.period" properties.
 */
typedef enum {
	RL_KAFKA_PRODUCE_ERR,
	RL_KAFKA_ERROR_CB,
	RL_KAFKA_DR_ERR,
	RL_NUM,
} rl_type_t;


static struct rate_limiter {
	uint64_t total;        /* Total number of events in current period */
	uint64_t suppressed;   /* Suppressed events in current period */
	const char *name;      /* Rate limiter log message summary */
	const char *fac;       /* Log facility */
} rate_limiters[RL_NUM] = {
	[RL_KAFKA_PRODUCE_ERR] = { .name = "Kafka produce errors",
				   .fac = "PRODUCE" },
	[RL_KAFKA_ERROR_CB] = { .name = "Kafka errors", .fac = "KAFKAERR" },
	[RL_KAFKA_DR_ERR] = { .name = "Kafka message delivery failures",
			      .fac = "KAFKADR" }
};

static time_t rate_limiter_t_curr; /* Current period */


/**
 * Roll over all rate limiters to a new period.
 */
static void rate_limiters_rollover (time_t now) {
	int i;

	for (i = 0 ; i < RL_NUM ; i++) {
		struct rate_limiter *rl = &rate_limiters[i];
		if (rl->suppressed > 0)
			vk_log(rl->fac, LOG_WARNING,
			       "Suppressed %"PRIu64" (out of %"PRIu64") %s",
			       rl->suppressed, rl->total, rl->name);
		rl->total = 0;
		rl->suppressed = 0;
	}

	rate_limiter_t_curr = now;
}


/**
 * Rate limiter.
 * Returns 1 if the threshold has been reached (DROP), or 0 if not (PASS)
 */
static int rate_limit (rl_type_t type) {
	struct rate_limiter *rl = &rate_limiters[type];

	if (++rl->total > conf.log_rate) {
		rl->suppressed++;
		return 1;
	}

	return 0;
}


/**
 * Kafka outputter
 */
void out_kafka (struct logline *lp, const char *buf, size_t len) {

	if (rd_kafka_produce(rkt, conf.partition, RD_KAFKA_MSG_F_COPY,
			     (void *)buf, len,
			     NULL, 0, NULL) == -1) {
		cnt.txerr++;
		if (!rate_limit(RL_KAFKA_PRODUCE_ERR))
			vk_log("PRODUCE", LOG_WARNING,
			       "Failed to produce Kafka message "
			       "(seq %"PRIu64"): %s (%i messages in outq)",
			       lp->seq, strerror(errno), rd_kafka_outq_len(rk));
	}

	rd_kafka_poll(rk, 0);
}


/**
 * Stdout outputter
 */
void out_stdout (struct logline *lp UNUSED, const char *buf, size_t len) {
	printf("%.*s\n", (int)len, buf);
}

/**
 * Null outputter
 */
void out_null (struct logline *lp UNUSED, const char *buf UNUSED, size_t len UNUSED) {
}


/**
 * Currently selected outputter.
 */
void (*outfunc) (struct logline *lp, const char *buf, size_t len) = out_kafka;


/**
 * Kafka error callback
 */
static void kafka_error_cb (rd_kafka_t *rk_arg UNUSED, int err,
			    const char *reason, void *opaque UNUSED) {
	if (!rate_limit(RL_KAFKA_ERROR_CB))
		vk_log("KAFKAERR", LOG_ERR,
		       "Kafka error (%i): %s", err, reason);
}


/**
 * Kafka message delivery report callback.
 * Called for each delivered (or failed delivery) message.
 * NOTE: If the dr callback is not to be used it can be turned off to
 *       improve performance.
 */
static void kafka_dr_cb (rd_kafka_t *rk_arg UNUSED,
			 void *payload UNUSED, size_t len,
			 int error_code,
			 void *opaque UNUSED, void *msg_opaque UNUSED) {
	_DBG("Kafka delivery report: error=%i, size=%zd", error_code, len);
	if (unlikely(error_code)) {
		cnt.kafka_drerr++;
		if (conf.log_kafka_msg_error && !rate_limit(RL_KAFKA_DR_ERR))
			vk_log("KAFKADR", LOG_NOTICE,
			       "Kafka message delivery error: %s",
			       rd_kafka_err2str(error_code));
	}
}

/**
 * Kafka statistics callback.
 */
static int kafka_stats_cb (rd_kafka_t *rk_arg UNUSED, char *json, size_t json_len UNUSED,
			    void *opaque UNUSED) {
	vk_log_stats("{ \"kafka\": %s }\n", json);
	return 0;
}


static void render_match_string (struct logline *lp) {
	char buf[8192];
	int  of = 0;
	int  i;

	/* Render each formatter in order. */
	for (i = 0 ; i < fconf.fmt_cnt ; i++) {
		const void *ptr;
		size_t len = lp->match[i].len;

		/* Either use accumulated value, or the default value. */
		if (len) {
			ptr = lp->match[i].ptr;
		} else {
			ptr = fconf.fmt[i].def;
			len = fconf.fmt[i].deflen;
		}

		if (of + len >= sizeof(buf))
			break;

		memcpy(buf+of, ptr, len);
		of += len;
	}

	/* Pass rendered log line to outputter function */
	cnt.tx++;
	outfunc(lp, buf, of);
}


static void render_match_json (struct logline *lp) {
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
	for (i = 0 ; i < fconf.fmt_cnt ; i++) {
		const void *ptr;
		size_t len = lp->match[i].len;

		/* Skip constant strings */
		if (fconf.fmt[i].id == 0)
			continue;

		/* Either use accumulated value, or the default value. */
		if (len) {
			ptr = lp->match[i].ptr;
		} else {
			ptr = fconf.fmt[i].def;
			len = fconf.fmt[i].deflen;
		}

		/* Field name */
		if (likely(fconf.fmt[i].name != NULL)) {
			yajl_gen_string(g, (const unsigned char *)fconf.fmt[i].name,
					fconf.fmt[i].namelen);
		} else {
			char name = (char)fconf.fmt[i].id;
			yajl_gen_string(g, (unsigned char *)&name, 1);
		}

		/* Value */
		switch (fconf.fmt[i].type)
		{
		case FMT_TYPE_STRING:
			yajl_gen_string(g, ptr, len);
			break;
		case FMT_TYPE_NUMBER:
			if (len == 3 && !strncasecmp(ptr, "nan", 3)) {
				/* There is no NaN in JSON, encode as null */
				ptr = "null";
				len = 4;
			}
			yajl_gen_number(g, ptr, len);
			break;
		}
	}

	yajl_gen_map_close(g);

	yajl_gen_get_buf(g, &buf, &buflen);

	/* Pass rendered log line to outputter function */
	outfunc(lp, (const char *)buf, buflen);

	yajl_gen_clear(g);
	yajl_gen_free(g);
}

/**
 * Render an accumulated logline to string and pass it to the output function.
 */
static void render_match (struct logline *lp, uint64_t seq) {
	lp->seq = seq;

	switch (fconf.encoding)
	{
	case VK_ENC_STRING:
		render_match_string(lp);
		break;
	case VK_ENC_JSON:
		render_match_json(lp);
		break;
	default:
		assert(0);
		break;
	}
}

/**
 * Resets the given logline and makes it ready for accumulating a new request.
 */
static void logline_reset (struct logline *lp) {
	/* Clear logline, except for scratch pad since it will be overwritten */
	memset(lp->match, 0, fconf.fmt_cnt * sizeof(*lp->match));
	lp->seq       = 0;
	lp->sof       = 0;
	lp->t_last    = time(NULL);
}


/**
 * Returns a new logline
 */
static struct logline *logline_get (void) {
	struct logline *lp;

	/* Allocate and set up new logline */
	lp = calloc(1, sizeof(*lp) + conf.scratch_size);
	lp->match = calloc(fconf.fmt_cnt, sizeof(*lp->match));

	return lp;
}


/**
 * Given a single tag 'tagid' with its data 'ptr' and 'len';
 * try to match it to the registered format tags.
 *
 * Returns 1 if the line is done and can be rendered, else 0.
 */
static int tag_match (struct logline *lp, int spec, enum VSL_tag_e tagid,
		      const char *ptr, size_t len) {
	const struct tag *tag;

	/* Iterate through all handlers for this tag. */
	for (tag = conf.tag[tagid] ; tag ; tag = tag->next) {
		const char *ptr2;
		size_t len2;

		/* Only use first tag data seen, unless TAG_F_LAST */
		if (!(tag->flags & TAG_F_LAST) && lp->match[tag->fmt->idx].ptr)
			continue;

		/* Match spec (client or backend) */
		if (!(tag->spec & spec))
			continue;

		if ((tag->var)
				&& !(tag->flags & TAG_F_TIMESTAMP)
				&& !(tag->flags & TAG_F_MATCH_PREFIX)) {
			const char *t;

			/* Get the occurence of ":" in ("Varname: value") */
			if (!(t = strnchr(ptr, len, ':')))
				continue;

			/* Variable match ("Varname: value") checks:
			 * 1) the len of the substring before the ':' (Varname) needs
			 *    to match the len of the tag requested.
			 * 2) strncasecmp between ptr (up to tag->varlen chars) and
			 *    the current tag candidate for the match
			 *	  must be 0 (so equal strings).
			 */
			if (tag->varlen != (int)(t-ptr) ||
			    strncasecmp(ptr, tag->var, tag->varlen))
				continue;

			if (likely(len > tag->varlen + 1 /* ":" */)) {
				ptr2 = t+1; /* ":" */
				/* Strip leading whitespaces */
				while (*ptr2 == ' ' && ptr2 < ptr+len)
					ptr2++;

				len2 = len - (int)(ptr2-ptr);
			} else {
				/* Empty value */
				len2 = 0;
				ptr2 = NULL;
			}

		} else if (tag->flags & TAG_F_TIMESTAMP) {
			/* The TAG_F_TIMESTAMP is related to the %{format}t
			 * output formatter available in the config.
			 * This formatter is special because tag->var gets populared
			 * with a string like '%FT%T', that represents a strftime
			 * compatible formatter, meanwhile there are multiple
			 * Varnish timestamp to choose (like Start, Resp, etc..) that
			 * can't be easily be matched comparing the Varnish tag read
			 * and tag->var. To add some flexibility, these are the
			 * allowed formats:
			 * 1) %{end:strftime_formatter}t
			 * 2) %{strftime_formatter}t
			 * The "end:" prefix will force Varnishkafka to pick the
			 * SLT_timestamp "Resp" timestamp, meanwhile the default is to
			 * use the "Start" one.
			 * Help to read the if conditions: strncmp returns 0 if the two
			 * strings are equal (up to some amount of chars) so the not
			 * operator is needed.
			 */
			if (!(tag->flags & TAG_F_TIMESTAMP_END)
				&& !strncmp(ptr, SLT_TIMESTAMP_START,
						strlen(SLT_TIMESTAMP_START))) {
				ptr2 = ptr;
				len2 = len;
			} else if ((tag->flags & TAG_F_TIMESTAMP_END)
					&& !strncmp(ptr, SLT_TIMESTAMP_RESP,
							strlen(SLT_TIMESTAMP_RESP))) {
				ptr2 = ptr;
				len2 = len;
			} else {
				continue;
			}
		} else if (tag->flags & TAG_F_MATCH_PREFIX) {
			/* If TAG_F_MATCH_PREFIX is used,  tag->var contains a generic
			 * prefix match.
			 * One example is the VSL tag, that emits strings like "timeout"
			 * or "store overflow", clearly not following the usual
			 * "$VAR:" format.
			 * Special use case is the '*' placeholder that will match every
			 * string.
			 */
			if ((tag->var && tag->var[0] == '*') ||
					!strncmp(ptr, tag->var, tag->varlen)) {
				ptr2 = ptr;
				len2 = len;
			} else {
				continue;
			}
		} else {
			ptr2 = ptr;
			len2 = len;
		}

		/* Get specified column if specified. */
		if (tag->col) {
			if (!column_get(tag->col, ' ', ptr2, len2, &ptr2, &len2)) {
				continue;
			}
		}

		if (tag->parser) {
			/* Pass value to parser which will assign it. */
			tag->parser(tag, lp, ptr2, len2);

		} else {
			/* Fallback to verbatim field. */
			match_assign(tag, lp, ptr2, len2, false);
		}

	}

	/* Request end: render the match string. */
	if (tagid == SLT_End)
		return 1;
	else
		return 0;
}



/**
 * A trasaction cursor (vsl.h) points to a list of tags associated with transaction id.
 * This function parses the current tag pointed by the cursor.
 */
static int parse_tag(struct logline* lp, struct VSL_transaction *t)
{
	/* Data carried by the transaction's current cursor */
	enum VSL_tag_e tag = VSL_TAG(t->c->rec.ptr);
	const char * tag_data = VSL_CDATA(t->c->rec.ptr);

	/* Avoiding VSL_LEN to prevent \0 termination char
	 * to be counted causing \u0000 to be displayed in JSON
	 * encodings.
	 */
	long len = strlen(tag_data);

	if (unlikely((!VSL_CLIENT(t->c->rec.ptr) &&
			(!VSL_BACKEND(t->c->rec.ptr)))))
		return conf.pret;

	/* Used by the parser to map Varnish tags with output placeholders.
	 * Currently VarnishKafka does not process backend tags (discarding the
	 * related transactions) so this field is not really used anymore, but
	 * it will be kept in case of future expansions.
	 */
	int spec = VSL_CLIENT(t->c->rec.ptr) ? VSL_CLIENTMARKER : VSL_BACKENDMARKER;

	/* Truncate data if exceeding configured max */
	if (unlikely(len > conf.tag_size_max)) {
		cnt.trunc++;
		len = conf.tag_size_max;
	}

	/* Accumulate matched tag content */
	if (likely(!tag_match(lp, spec, tag, tag_data, len)))
		return conf.pret;

	/* Log line is complete: render & output (stdout or kafka) */
	render_match(lp, ++conf.sequence_number);

	/* clean up */
	logline_reset(lp);

	/* Reuse fresh timestamp lp->t_last from logline_reset() */
	if (unlikely(lp->t_last >= rate_limiter_t_curr + conf.log_rate_period))
		rate_limiters_rollover(lp->t_last);

	/* Stats output */
	if (conf.stats_interval) {
		if (unlikely(conf.need_logrotate)) {
			logrotate();
		}
		if (unlikely(lp->t_last >= conf.t_last_stats + conf.stats_interval)) {
			print_stats();
			conf.t_last_stats = lp->t_last;
		}
	}

	return conf.pret;
}


/**
 * VSL_Dispatch() callback called for each transaction group read from the VSL.
 * A transaction is a collection of tags indicating actions performed by Varnish
 * (see https://www.varnish-cache.org/docs/4.1/reference/vsl.html). The grouping
 * used in varnish-kafka is by request, so for example each backend request triggered
 * by a single client request will have a different transaction id (and tags) but
 * will reference the same parent transaction (the main request).
 */
static int __match_proto__(VSLQ_dispatch_f) transaction_scribe (struct VSL_data *vsl UNUSED,
		struct VSL_transaction * const pt[], void *priv) {
	struct logline* lp = priv;
	struct VSL_transaction *t;
	/* Loop through the transations of the grouping */
	while ((t = *pt++)) {
		/* Only client requests are allowed */
		if (t->type != VSL_t_req)
			continue;
		if (t->reason == VSL_r_esi)
			continue;
		/* loop through the tags */
		while (VSL_Next(t->c) == 1) {
			parse_tag(lp, t);
		}
	}
	return 0;
}



/**
 * varnishkafka logger
 */
void vk_log0 (const char *func UNUSED, const char *file UNUSED, int line UNUSED,
	      const char *facility, int level, const char *fmt, ...) {
	va_list ap;
	static char buf[8192];

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
 * Appends a formatted string to the conf.stats_fp file.
 * conf.stats_fp is configured using the log.statistics.file property.
 */
void vk_log_stats(const char *fmt, ...) {
	va_list ap;

	/* If stats_fp is 0, vk_log_stats() shouldn't have been called */
	if (!conf.stats_fp)
		return;

	/* Check if stats_fp needs rotating.
	   This will usually already be taken care
	   of by the check in parse_tag, but we
	   do it here just in case we need to rotate
	   and someone else called vk_log_stats,
	   e.g. kafka_stats_cb.
	*/
	if (unlikely(conf.need_logrotate)) {
		logrotate();
	}

	va_start(ap, fmt);
	vfprintf(conf.stats_fp, fmt, ap);
	va_end(ap);

	/* flush stats_fp to make sure valid JSON data
	   (e.g. full lines with closing object brackets)
	   is written to disk */
	if (fflush(conf.stats_fp)) {
		vk_log("STATS", LOG_ERR,
			"Failed to fflush log.statistics.file %s: %s",
			conf.stats_file, strerror(errno));
	}
}


/**
 * Closes and reopens any open logging file pointers.
 * This should be called not from the SIGHUP handler, but
 * instead from somewhere in a main execution loop.
 */
static void logrotate(void) {
	fclose(conf.stats_fp);

	if (!(conf.stats_fp = fopen(conf.stats_file, "a"))) {
		vk_log("STATS", LOG_ERR,
			"Failed to reopen log.statistics.file %s after logrotate: %s",
			conf.stats_file, strerror(errno));
	}

	conf.need_logrotate = 0;
}


/**
 * Hangup signal handler.
 * Sets the global logratate variable to 1 to indicate
 * that any open file handles should be closed and reopened
 * as soon as possible.
 *
 */
static void sig_hup(int sig UNUSED) {
	conf.need_logrotate = 1;
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
		" Usage: %s [-S <config-file>] [-n <varnishd instance>] "
		" [-N VSM filename] [-q VSL query] [-D daemonize] [-T VSL timeout seconds] "
		" [-L VSL transactions upper limit]\n"
		"\n"
		" Args can also be set through the configuration file "
		" (check the default configuration file for examples).\n"
		"\n"
		" Default configuration file path: %s\n"
		"\n",
		VARNISHKAFKA_VERSION,
		argv0,
		VARNISHKAFKA_CONF_PATH);
	exit(1);
}


static void varnish_api_cleaning(void) {
	if (conf.vslq)
		VSLQ_Delete(&conf.vslq);

	if (conf.vsl)
		VSL_Delete(conf.vsl);

	if (conf.vsm)
		VSM_Delete(conf.vsm);

}

/**
 * Open and configure VSM/VSL/VSLQ settings. The vslq_query parameter will be
 * used to know if a VSLQ query/filter needs to be set or not.
 * Returns 0 in case of success, -1 otherwise.
 */
static int varnish_api_open_handles(struct VSM_data **vsm, struct VSL_data **vsl,
				    struct VSL_cursor **vsl_cursor,
				    unsigned int vsl_cursor_options,
				    struct VSLQ **vslq, char* vslq_query) {
	if (VSM_Open(*vsm) < 0) {
		vk_log("VSM_OPEN", LOG_DEBUG, "Failed to open Varnish VSL: %s\n", VSM_Error(*vsm));
		return -1;
	}
	*vsl_cursor = VSL_CursorVSM(*vsl, *vsm, vsl_cursor_options);
	if (*vsl_cursor == NULL) {
		vk_log("VSL_CursorVSM", LOG_DEBUG, "Failed to obtain a cursor for the SHM log: %s\n",
			VSL_Error(*vsl));
		return -1;
	}
	/* Setting VSLQ query */
	if (vslq_query) {
		*vslq = VSLQ_New(*vsl, vsl_cursor, VSL_g_request, vslq_query);
	} else {
		*vslq = VSLQ_New(*vsl, vsl_cursor, VSL_g_request, NULL);
	}
	if (*vslq == NULL) {
		vk_log("VSLQ_NEW", LOG_DEBUG, "Failed to instantiate the VSL query: %s\n",
			VSL_Error(*vsl));
		return -1;
	}
	return 0;
}


int main (int argc, char **argv) {
	char errstr[4096];
	char hostname[1024];
	struct hostent *lh;
	char c;

	/*
	 * Default configuration
	 */
	conf.log_level = 6;
	conf.log_to    = VK_LOG_STDERR;
	conf.log_rate  = 100;
	conf.log_rate_period = 60;
	conf.daemonize = 1;
	conf.tag_size_max   = 2048;
	conf.scratch_size   = 4 * 1024 * 1024; // 4MB
	conf.stats_interval = 60;
	conf.stats_file     = strdup("/tmp/varnishkafka.stats.json");
	conf.log_kafka_msg_error = 1;
	conf.rk_conf = rd_kafka_conf_new();
	rd_kafka_conf_set(conf.rk_conf, "client.id", "varnishkafka", NULL, 0);
	rd_kafka_conf_set_error_cb(conf.rk_conf, kafka_error_cb);
	rd_kafka_conf_set_dr_cb(conf.rk_conf, kafka_dr_cb);
	rd_kafka_conf_set(conf.rk_conf, "queue.buffering.max.messages",
			  "1000000", NULL, 0);

	conf.topic_conf = rd_kafka_topic_conf_new();
	rd_kafka_topic_conf_set(conf.topic_conf, "required_acks", "1", NULL, 0);

	conf.format = "%l %n %t %{Varnish:time_firstbyte}x %h "
		"%{Varnish:handling}x/%s %b %m http://%{Host}i%U%q - - "
		"%{Referer}i %{X-Forwarded-For}i %{User-agent}i";

	/* Construct logname (%l) from local hostname */
	gethostname(hostname, sizeof(hostname)-1);
	hostname[sizeof(hostname)-1] = '\0';
	lh = gethostbyname(hostname);
	conf.logname = strdup(lh->h_name);

	/* Parse command line arguments */
	while ((c = getopt(argc, argv, "hS:N:Dq:n:T:L:")) != -1) {
		switch (c) {
		case 'h':
			usage(argv[0]);
			break;
		case 'S':
			/* varnish-kafka config filepath */
			conf_file_path = optarg;
			break;
		case 'N':
			/* Open a specific shm file */
			conf.N_flag = 1;
			conf.N_flag_path = strdup(optarg);
			break;
		case 'D':
			conf.daemonize = 1;
			break;
		case 'q':
			/* VSLQ query */
			conf.q_flag = 1;
			conf.q_flag_query = strdup(optarg);
			break;
		case 'n':
			/* name of varnishd instance to use */
			conf.n_flag = 1;
			conf.n_flag_name = strdup(optarg);
			break;
		case 'T':
			/* Maximum (VSL) wait time (seconds) between a Begin tag and a End one.
			 * Varnish workers write log tags to a buffer that gets flushed
			 * to the shmlog once full. It might happen that a Begin
			 * tag gets flushed to shmlog as part of a batch without
			 * its correspondent End tag (for example, due to long requests).
			 * Consistency checks for the value postponed
			 * in the VSL_Arg function later on.
			 * VSL default is 120.
			 */
			conf.T_flag = 1;
			conf.T_flag_seconds = strdup(optarg);
			break;
		case 'L':
			/* Upper limit of incomplete VSL transactions kept before
			 * the oldest one is force completed.
			 * Consistency checks for the value postponed
			 * in the VSL_Arg function later on.
			 * VSL default is 1000.
			 */
			 conf.L_flag = 1;
			 conf.L_flag_transactions = strdup(optarg);
			 break;
		default:
			usage(argv[0]);
			break;
		}
	}

	/* Read config file */
	if (conf_file_read(conf_file_path) == -1)
		exit(1);

	if (!conf.topic)
		usage(argv[0]);

	/* Set up syslog */
	if (conf.log_to & VK_LOG_SYSLOG)
		openlog("varnishkafka", LOG_PID|LOG_NDELAY, LOG_DAEMON);

	/* Set up statistics gathering in librdkafka, if enabled. */
	if (conf.stats_interval) {
		char tmp[30];

		if (!(conf.stats_fp = fopen(conf.stats_file, "a"))) {
			fprintf(stderr, "Failed to open statistics log file %s: %s\n",
				conf.stats_file, strerror(errno));
			exit(1);
		}

		snprintf(tmp, sizeof(tmp), "%i", conf.stats_interval*1000);
		rd_kafka_conf_set_stats_cb(conf.rk_conf, kafka_stats_cb);
		rd_kafka_conf_set(conf.rk_conf, "statistics.interval.ms", tmp,
				  NULL, 0);

		/* Install SIGHUP handler for logrotating stats_fp. */
		signal(SIGHUP, sig_hup);
	}

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

	/* Parse the format string */
	if(!conf.format) {
		vk_log("FMT", LOG_ERR, "No formats defined");
		exit(1);
	}

	if (format_parse(conf.format,
			 errstr, sizeof(errstr)) == -1) {
		vk_log("FMTPARSE", LOG_ERR,
		       "Failed to parse Main format string: %s\n%s",
		       conf.format, errstr);
		exit(1);
	}

	if (conf.log_level >= 7)
		tag_dump();

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

	/* Varnish VSL declaration (vsm and vsl structures) is done
	 * in the header file because used in both config.c and varnishkafka.c
	 */
	conf.vsl = VSL_New();
	struct VSL_cursor *vsl_cursor = NULL;
	conf.vsm = VSM_New();

	if (conf.T_flag) {
		if (VSL_Arg(conf.vsl, 'T', conf.T_flag_seconds) < 0) {
			vk_log("VSL_T_arg", LOG_ERR, "Failed to set a %s timeout for VSL log transactions: %s",
					conf.T_flag_seconds, VSL_Error(conf.vsl));
			varnish_api_cleaning();
			exit(1);
		}
	}

	if (conf.L_flag) {
		if (VSL_Arg(conf.vsl, 'L', conf.L_flag_transactions) < 0) {
			vk_log("VSL_L_arg", LOG_ERR, "Failed to set a %s upper limit of VSL log transactions: %s",
					conf.L_flag_transactions, VSL_Error(conf.vsl));
			varnish_api_cleaning();
			exit(1);
		}
	}

	/* Check if the user wants to open a specific SHM File (-N) or
	 * a SHM file related to a specific varnishd instance (-n)
	 */
	if (conf.N_flag) {
		if (!VSM_N_Arg(conf.vsm, conf.N_flag_path)) {
			vk_log("VSM_N_arg", LOG_ERR, "Failed to open %s: %s",
					conf.N_flag_path, VSM_Error(conf.vsm));
			varnish_api_cleaning();
			exit(1);
		}
	} else if (conf.n_flag) {
		if (!VSM_n_Arg(conf.vsm, conf.n_flag_name)) {
			vk_log("VSM_n_arg", LOG_ERR, "Failed to open shm for varnishd %s: %s",
					conf.n_flag_name, VSM_Error(conf.vsm));
			varnish_api_cleaning();
			exit(1);
		}
	}

	/* Main dispatcher loop depending on outputter */
	conf.run = 1;
	conf.pret = 0;

	/* time struct to sleep for 10ms */
	struct timespec duration_10ms;
	duration_10ms.tv_sec = 0;
	duration_10ms.tv_nsec = 10000000L;

	/* time struct to sleep for 100ms */
	struct timespec duration_100ms;
	duration_100ms.tv_sec = 0;
	duration_100ms.tv_nsec = 100000000L;

	/* Creating a new logline (will be re-used across log transactions) */
	struct logline *lp = NULL;
	if (unlikely(!(lp = logline_get())))
		return -1;

	while (conf.run) {
		/* Attempt to connect to the shm log handle if not open. Varnishkafka
		 * will try to connect periodically to the shm log until it gets one,
		 * or it will keep trying endlessly.
		 * Two use cases:
		 * - varnishkafka is started but varnish is not. This behavior is
		 *   useful if there is no strict start ordering for varnish and varnishkafka.
		 * - varnish gets restarted and the shm log handle is not valid anymore.
		 */
		if (conf.vsm != NULL && !VSM_IsOpen(conf.vsm)) {
			if (varnish_api_open_handles(&conf.vsm, &conf.vsl, &vsl_cursor,
						     VSL_COPT_TAIL | VSL_COPT_BATCH, &conf.vslq,
						     conf.q_flag_query) == -1) {
				nanosleep(&duration_100ms, NULL);
				continue;
			} else {
				vk_log("VSLQ_Dispatch", LOG_ERR, "Log acquired!");
				/* Setting the sequence number back to zero to track
				 * the fact that Varnish abandoned the log, probably due to
				 * a restart (or simply that varnishkafka started before varnish).
				 */
				conf.sequence_number = conf.sequence_number_start;
			}
		}

		int dispatch_status = VSLQ_Dispatch(conf.vslq, transaction_scribe, lp);

		/* Nothing to read from the shm handle, sleeping */
		if (dispatch_status == 0)
			nanosleep(&duration_10ms, NULL);

		/* In case of Varnish log abandoned or overrun, the handle needs
		 * to be closed to allow a reconnect during the next while cycle
		 */
		else if (dispatch_status <= -2) {
			vk_log("VSLQ_Dispatch", LOG_ERR, "Varnish Log abandoned or overrun.");
			VSM_Close(conf.vsm);
		}

		/* EOF from the Varnish Log, closing gracefully */
		else if (dispatch_status == -1) {
			vk_log("VSLQ_Dispatch", LOG_ERR, "Varnish Log EOF.");
			break;
		}

		if (outfunc == out_kafka)
			rd_kafka_poll(rk, 0);
	}

	/* Run until all kafka messages have been delivered
	* or we are stopped again */
	conf.run = 1;

	if (outfunc == out_kafka) {
		/* Check if all the messages have been delivered
		 * to Kafka to update statistics.
		 */
		while (conf.run && (rd_kafka_outq_len(rk) > 0))
			rd_kafka_poll(rk, 100);

		/* Kafka clean-up */
		rd_kafka_destroy(rk);
	}

	print_stats();

	/* if stats_fp is set (i.e. open), close it. */
	if (conf.stats_fp) {
		fclose(conf.stats_fp);
		conf.stats_fp = NULL;
	}

	free(conf.stats_file);

	free(lp);

	rate_limiters_rollover(time(NULL));

	varnish_api_cleaning();

	exit(0);
}
