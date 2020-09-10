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


#define _ISOC99_SOURCE  /* for strtoull() */

#include <string.h>
#include <strings.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <ctype.h>
#include <stdlib.h>
#include <time.h>

#include <librdkafka/rdkafka.h>
#include "varnishkafka.h"

/**
 * varnishkafka global configuration
 */
struct conf conf;
struct fmt_conf fconf;


/**
 * Parses the value as true or false.
 */
static int conf_tof (const char *val) {
    char *end;
    int i;

    i = strtoul(val, &end, 0);
    if (end > val) /* treat as integer value */
        return !!i;

    if (!strcasecmp(val, "yes") ||
        !strcasecmp(val, "true") ||
        !strcasecmp(val, "on"))
        return 1;
    else
        return 0;
}


static fmt_enc_t encoding_parse (const char *val) {
    if (!strcasecmp(val, "string"))
        return VK_ENC_STRING;
    else if (!strcasecmp(val, "json"))
        return VK_ENC_JSON;
    else
        return VK_ENC_INVALID;
}


/**
 * Set a single configuration property 'name' using value 'val'.
 * Returns 0 on success, and -1 on error in which case 'errstr' will
 * contain an error string.
 */
static int conf_set (const char *name, const char *val,
                     char *errstr, size_t errstr_size) {
    rd_kafka_conf_res_t res;


    /* Kafka configuration */
    if (!strncmp(name, "kafka.", strlen("kafka."))) {
        name += strlen("kafka.");

        /* Kafka topic configuration. */
        if (!strncmp(name, "topic.", strlen("topic.")))
            res = rd_kafka_topic_conf_set(conf.topic_conf, name+strlen("topic."),
                                          val, errstr, errstr_size);
        else /* Kafka global configuration */
            res = rd_kafka_conf_set(conf.rk_conf, name,
                        val, errstr, errstr_size);

        if (res == RD_KAFKA_CONF_OK)
            return 0;
        else if (res != RD_KAFKA_CONF_UNKNOWN)
            return -1;

        /* Unknown configs: fall thru */
        name -= strlen("kafka.");
    }

    /* librdkafka handles NULL configuration values, we dont. */
    if (!val) {
        snprintf(errstr, errstr_size, "\"%s\" requires an argument", name);
        return -1;
    }

    /* varnishkafka configuration options */
    if (!strcmp(name, "kafka.topic"))
        conf.topic = strdup(val);
    else if (!strcmp(name, "kafka.partition"))
        conf.partition = atoi(val);
    else if (!strcmp(name, "format"))
        conf.format = strdup(val);
    else if (!strcmp(name, "format.type")) {
        if ((fconf.encoding =
             encoding_parse(val)) == VK_ENC_INVALID) {
            snprintf(errstr, errstr_size,
                 "Unknown format.type value \"%s\"", val);
            return -1;
        }
    } else if (!strcmp(name, "tag.size.max"))
        conf.tag_size_max = atoi(val);
    else if (!strcmp(name, "log.level"))
        conf.log_level = atoi(val);
    else if (!strcmp(name, "log.stderr")) {
        if (conf_tof(val))
            conf.log_to |= VK_LOG_STDERR;
        else
            conf.log_to &= ~VK_LOG_STDERR;
    } else if (!strcmp(name, "log.syslog")) {
        if (conf_tof(val))
            conf.log_to |= VK_LOG_SYSLOG;
        else
            conf.log_to &= ~VK_LOG_SYSLOG;
    } else if (!strcmp(name, "log.kafka.msg.error"))
        conf.log_kafka_msg_error = conf_tof(val);
    else if (!strcmp(name, "log.statistics.file")) {
        free(conf.stats_file);
        conf.stats_file = strdup(val);
    } else if (!strcmp(name, "log.statistics.interval"))
        conf.stats_interval = atoi(val);
    else if (!strcmp(name, "log.rate.max"))
        conf.log_rate = atoi(val);
    else if (!strcmp(name, "log.rate.period"))
        conf.log_rate_period = atoi(val);
    else if (!strcmp(name, "daemonize"))
        conf.daemonize = conf_tof(val);
    else if (!strcmp(name, "sequence.number")) {
        if (!strcmp(val, "time"))
            conf.sequence_number = (uint64_t)time(NULL)*1000000llu;
        else
            conf.sequence_number = strtoull(val, NULL, 0);
        conf.sequence_number_start = conf.sequence_number;
    } else if (!strcmp(name, "output")) {
        if (!strcmp(val, "kafka"))
            outfunc = out_kafka;
        else if (!strcmp(val, "-") || !strcmp(val, "stdout"))
            outfunc = out_stdout;
        else if (!strcmp(val, "null"))
            outfunc = out_null;
        else {
            snprintf(errstr, errstr_size,
                 "Unknown outputter \"%s\": "
                 "try \"stdout\" or \"kafka\"", val);
            return -1;
        }
    } else if (!strcmp(name, "logline.scratch.size"))
        conf.scratch_size = atoi(val);
    else if (!strcmp(name, "varnish.arg.q")) {
        conf.q_flag = 1;
        conf.q_flag_query = strdup(val);
    } else if (!strcmp(name, "varnish.arg.n")) {
        conf.n_flag = 1;
        conf.n_flag_name = strdup(val);
    } else if (!strcmp(name, "varnish.arg.T")) {
        conf.T_flag = 1;
        conf.T_flag_seconds = strdup(val);
    } else if (!strcmp(name, "varnish.arg.L")) {
        conf.L_flag = 1;
        conf.L_flag_transactions = strdup(val);
    } else {
        snprintf(errstr, errstr_size,
             "Unknown configuration property \"%s\"\n", name);
        return -1;
    }
    return 0;
}


/* Left and right trim string '*sp' of white spaces (incl newlines). */
static int trim (char **sp, char *end) {
    char *s = *sp;

    while (s < end && isspace(*s))
        s++;

    end--;

    while (end > s && isspace(*end)) {
        *end = '\0';
        end--;
    }

    *sp = s;

    return (int)(end - *sp);
}


/**
 * Read and parse the supplied configuration file.
 * Returns 0 on success or -1 on failure.
 */
int conf_file_read (const char *path) {
    FILE *fp;
    char buf[4096];
    char errstr[4096];
    int line = 0;

    if (!(fp = fopen(path, "r"))) {
        fprintf(stderr, "Failed to open configuration file %s: %s\n",
            path, strerror(errno));
        return -1;
    }

    while (fgets(buf, sizeof(buf), fp)) {
        char *s = buf;
        char *t;

        line++;

        while (isspace(*s))
            s++;

        if (!*s || *s == '#')
            continue;

        /* "name=value"
         * find ^      */
        if (!(t = strchr(s, '='))) {
            fprintf(stderr,
                "%s:%i: warning: "
                "missing '=': line ignored\n",
                path, line);
            continue;
        }

        /* trim "name"=.. */
        if (!trim(&s, t)) {
            fprintf(stderr, "%s:%i: warning: empty left-hand-side\n", path, line);
            continue;
        }

        /* terminate "name"=.. */
        *t = '\0';
        t++;

        /* trim ..="value" */
        trim(&t, t + strlen(t));

        /* set the configuration vlaue. */
        if (conf_set(s, *t ? t : NULL, errstr, sizeof(errstr)) == -1) {
            fprintf(stderr, "%s:%i: error: %s\n",
                path, line, errstr);
            fclose(fp);
            return -1;
        }
    }

    fclose(fp);
    return 0;
}
