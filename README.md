varnishkafka - varnish log collector with Apache Kafka integration
==================================================================

Copyright (c) 2013 [Wikimedia Foundation](http://www.wikimedia.org)

Copyright (c) 2013 [Magnus Edenhill](http://www.edenhill.se/)

[https://github.com/wikimedia/varnishkafka](https://github.com/wikimedia/varnishkafka)

# Description

**varnishkafka** is a varnish log collector with an integrated Apache Kafka
producer.
It was written from scratch with performance and modularity in mind,
**varnishkafka** consumes about a third of the CPU that varnishncsa does and
has a far more frugal memory approach.

# Supported outputs and formats

Currently supported outputs:

 * kafka  - produce messages to one or more Apache Kafka brokers
 * stdout - write log messages to stdout (as varnishncsa does)

Currently supported output formats:

 * string - arbitrary text output according to the configured formatting.
 * json   - output as JSON with configurable field, field types and names.

New formats and outputs can easily be added.

# Configuration

**varnishkafka** is configured with a configuration file and is designed
to operate as a system daemon.

Please see the configuration file example,
*varnishkafka.conf.example*, for a full description of configuration properties.

The standard Varnish VSL command line arguments are supposed, both through
the command line and configuration file.

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

    # Read offline log file
    varnishkafka -r varnishlog.vsl

    # Usage description
    varnishkafka -h
