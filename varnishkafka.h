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

#pragma once

#ifndef likely
#define likely(x)   __builtin_expect((x),1)
#endif
#ifndef unlikely
#define unlikely(x) __builtin_expect((x),0)
#endif




#define VSL_TAGS_MAX 255
/* A tag that is in all requests but only once. */
#define VSL_TAG__ONCE  SLT_ReqEnd


/**
 * Pointer to matched tag's content.
 */
struct match {
	const char *ptr;
	int         len;
};

/**
 * Currently parsed logline(s)
 */
struct logline {
	/* Match state */
	struct match *match;

	/* Tags seen (for -m regexp) */
	uint64_t tags_seen;

	/* Sequence number */
	uint64_t seq;

	/* Scratch pad */
	int      sof;
	char     scratch[2048];  /* Must be at end of struct */
};


/**
 * Tag found in format.
 */
struct tag {
	struct tag *next;
	struct fmt *fmt;
	int    spec;
	int    tag;
	char  *var;
	int    varlen;
	int  (*parser) (const struct tag *tag, struct logline *lp,
			const char *ptr, int len);
	int    col;
};

/**
 * Formatting from format
 */
struct fmt {
	int   id;         /* formatter (i.e., (char)'r' in "%r") */
	int   idx;        /* fmt[] array index */
	const char *var;  /* variable name  (for %{..}x,i,o) */
	const char *def;  /* default string, typically "-" */
	int   deflen;     /* default string's length */
	const char *name; /* field name (for JSON, et.al) */
	int   namelen;    /* name length */
	enum {
		FMT_TYPE_STRING,
		FMT_TYPE_NUMBER,
	}     type;       /* output type (for JSON, et.al) */
	int   flags;
#define FMT_F_ESCAPE    0x1 /* Escape the value string */
};



/**
 * varnishkafka config & state struct
 *
 * Try to keep commonly used fields at the top.
 */
struct conf {
	int         run;
	int         pret;   /* parse return value: use to exit parser. */
	int         m_flag;

	/* Sparsely populated with desired tags */
	struct tag **tag;

	/* Array of tags in output order. */
	struct fmt *fmt;
	int         fmt_cnt;
	int         fmt_size;

	uint64_t    sequence_number;

	int         datacopy;
	enum {
		VK_FORMAT_STRING,
		VK_FORMAT_JSON,
		VK_FORMAT_PROTOBUF,
		VK_FORMAT_AVRO,
	} format_type;
	
	/* Kafka config */
	int         partition;
	char       *topic;

	char       *logname;

	int         log_level;

	int         log_to;
#define VK_LOG_STDERR 0x1
#define VK_LOG_SYSLOG 0x2

	char       *format;
	int         daemonize;

	rd_kafka_conf_t       rk_conf;
	rd_kafka_topic_conf_t topic_conf;
};

extern struct conf conf;
struct VSM_data *vd;


int conf_file_read (const char *path);


void vk_log0 (const char *func, const char *file, int line,
	      const char *facility, int level, const char *fmt, ...)
	__attribute__((format (printf, 6, 7)));

#define vk_log(facility,level,fmt...) \
	vk_log0(__FUNCTION__,__FILE__,__LINE__, facility, level, fmt)

#define _DBG(fmt...) vk_log("DEBUG", LOG_DEBUG, fmt)


void out_kafka (const char *buf, size_t len);
void out_stdout (const char *buf, size_t len);
void out_null (const char *buf, size_t len);
extern void (*outfunc) (const char *buf, size_t len);
