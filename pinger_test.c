

#include "pinger.h"

#include <unistd.h>
#include <assert.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <string.h>

static int get_addr(char *dst, struct sockaddr *addr)
{
	struct addrinfo *res;
	int ret;

	ret = getaddrinfo(dst, NULL, NULL, &res);
	if (ret) {
		printf("getaddrinfo failed (%s) - invalid hostname or IP address\n", gai_strerror(ret));
		return ret;
	}

	if (res->ai_family == PF_INET)
		memcpy(addr, res->ai_addr, sizeof(struct sockaddr_in));
	else if (res->ai_family == PF_INET6)
		memcpy(addr, res->ai_addr, sizeof(struct sockaddr_in6));
	else
		ret = -1;

	freeaddrinfo(res);
	return ret;
}

int main(int argc, char *argv[])
{
	int ret;
	struct pinger_attr attr;
	struct pinger_peer_attr peer_attr;
	pinger_t pinger;
	pinger_pid_t peer;
	pinger_rtt_t rtt;

	if (argc < 3) {
		printf("pinger_test <source IP> <dest IP>\n");
		return -1;
	}
	printf("using src addr: %s dst_addr: %s\n", argv[1], argv[2]);

	ret = get_addr(argv[1], (struct sockaddr *) &attr.sin);
	assert(!ret);
	attr.port = 13007;
	attr.npeers = 5;
	attr.ping_interval_us = 1000000;

	ret = pinger_create(&attr, &pinger);
	assert(!ret);

	printf("created pinger\n");

	get_addr(argv[2], (struct sockaddr *) &peer_attr.sin);
	peer_attr.port = 13007;
	ret = pinger_connect(pinger, &peer_attr, &peer);
	assert(!ret);

	printf("connected peer\n");

	int i;
	for (i = 0; i < 20; i++) {
		ret = pinger_query(pinger, peer, &rtt);
		assert(!ret);
		sleep(1);
	}

	printf("queried peer: %d val: %f\n", peer, rtt);

	ret = pinger_destroy(pinger);
	assert(!ret);

	printf("destroyed pinger\n");

	return 0;
}
