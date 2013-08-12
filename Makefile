
PROG	 = varnishkafka
SRCS	 = varnishkafka.c config.c base64.c

DESTDIR?=/usr/local

# Let packagers override version and default configuration file path
# through VER and CFPATH env variables.
ifeq (,${VER})
VER     := `git describe --abbrev=6 --tags HEAD`
endif

ifeq (,${CFPATH})
CFPATH  := /etc/varnishkafka.conf
endif

CFLAGS  += -DVARNISHKAFKA_VERSION=\"$(VER)\"
CFLAGS  += -DVARNISHKAFKA_CONF_PATH=\"$(CFPATH)\"

CFLAGS	+= -Wall -Werror -O2 -g 
LIBS    += -lrdkafka -lvarnishapi -lpthread


all:
	gcc $(CFLAGS) $(SRCS) -o $(PROG) $(LIBS)


install:
	if [ "$(DESTDIR)" != "/usr/local" ]; then \
		DESTDIR="$(DESTDIR)/usr"; \
	else \
		DESTDIR="$(DESTDIR)" ; \
	fi ; \
	install -t $${DESTDIR}/bin $(PROG)


clean:
	rm -f *.o $(PROG)
