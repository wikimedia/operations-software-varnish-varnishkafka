varnishkafka - varnish log collector with Apache Kafka integration
==================================================================

Copyright (c) 2013 [Wikimedia Foundation](http://www.wikimedia.org)

Copyright (c) 2013 [Magnus Edenhill](http://www.edenhill.se/)

[https://github.com/wikimedia/varnishkafka](https://github.com/wikimedia/varnishkafka)

# Description

**varnishkafka** is a varnish log collector with an integrated Apache Kafka
producer.
It was written from scratch with performance and modularity in mind.

# Supported outputs and formats

Currently supported outputs:

 * kafka  - produce messages to one or more Apache Kafka brokers
 * stdout - write log messages to stdout (as varnishncsa does)

Currently supported output formats:

 * string - arbitrary text output according to the configured formatting.
 * json   - output as JSON with configurable field, field types and names.

New formats and outputs can easily be added.

# Varnish compatibility

Varnishkafka is fully compatible with the Varnish 4 and 5.1 APIs, but since they changed
a lot from the previous version we had to break compatibility with Varnish 3.
If you want to use Varnishkafka with Varnish 3, please check the related branch.
New features will be added only to this branch from now on.

# Important notes about internal settings

The initial position of the Varnish log cursor is fixed and set to start
from the tail of the log. This implies that varnishkafka collects log entries
that occur while it is running.

# Configuration

**varnishkafka** is configured with a configuration file and is designed
to operate as a system daemon.

Please see the configuration file example,
*varnishkafka.conf.example*, for a full description of configuration properties.

The standard Varnish VSL command line arguments are supported, both through
the command line and configuration file.

## TLS/SSL Support

**varnishkafka** leverages TLS support offered by **librdkafka** simply passing
the right configuration parameters to it as indicated in this tutorial:
[Using-SSL-with-librdkafka](https://github.com/edenhill/librdkafka/wiki/Using-SSL-with-librdkafka).

It requires a recent (>= 0.9) version of **librdkafka**. Please check the TLS/SSL section
of *varnishkafka.conf.example* for more information.

# License

**varnishkafka** is licensed under the 2-clause BSD license.


The Apache Kafka support is provided by [librdkafka](https://github.com/edenhill/librdkafka)


# Usage

## Requirements
	libvarnishapi
	librdkafka
	libyajl
	pthreads
	zlib
	libm

Please note that the entire codebase assumes the use of glibc, so other C library implementations
might not compile or work as expected.

## Instructions

### Building

      make
      sudo make install
      # or to install in another location than /usr/local, set DESTDIR env
      # to the filesystem root of your choice.
      sudo make DESTDIR=/usr make install


### Run

    # If /etc/varnishkafka.conf exists
    varnishkafka

    # Alternative configuration path
    varnishkafka -S /path/to/varnishkafka.conf

    # Read a specific log file
    varnishkafka -S /path/to/varnishkafka.conf -N /path/to/_.vsm

    # Read a specific Varnish instance log file
    varnishkafka -S /path/to/varnishkafka.conf -n varnish_instance_name

    # Usage description
    varnishkafka -h
