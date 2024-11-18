CC = gcc
CFLAGS += -g 
#CFLAGS += -O2 -Wall -W -Werror
LDFLAGS += -libverbs -lrdmacm -lmlx5
TARGETS = dcping

all: pinger
	$(CC) $(CFLAGS) -o $(TARGETS) dcping.c $(LDFLAGS)

pinger:
	$(CC) $(CFLAGS) $(LDFLAGS) -pthread -o pinger_test pinger.c pinger_test.c

clean:
	rm -f $(TARGETS)
	rm -f pinger_test
