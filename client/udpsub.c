/*
 * cc udpsub.c -o udpsub -lzmq
 *
 * simple client to write subscriber's message to stdout
 *
 * example: ./udpsub -s ipc:///tmp/serv.ipc
 *
 * mm@dharma
 *
 */

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <stdarg.h>
#include <stdlib.h>
#include <unistd.h>
#include <limits.h>
#include <errno.h>

#include <zmq.h>
        
int done;

static void 
usage(const char *name)
{
	printf(
		"\n%s\n"
		"\t-b bsize buffer size for zmq_recv() default: 1500\n"
		"\t-f filtername set filter for subscribe\n"
		"\t-s pathname	path name eg. ipc:///tmp/serv.ipc\n"
		"\t-h messages  set ZMQ_RCVHWM default 0 (unlimited)\n"
		"example: %s -s ipc:///tmp/serv.ipc\n\n",	
		name, name);	
}

static void
catchsig()
{
	done = 1;
}

int
main (int argc, char *argv[])
{
	char path[PATH_MAX] = "\0";
	char filter[255] = "";
	struct sigaction action;
        char *buf;
	void *context, *subscriber;
        int n, opt;
	int havepath = 0;
	int maxrcvmsg = 0;
	size_t bsize = 1500;

	extern char *optarg;


	while ((opt = getopt(argc, argv, "f:b:h:s:")) != -1)
		switch (opt) {
			case 'b':
                                if (sscanf(optarg, "%zd", &bsize) != 1) {
                                        perror("sscanf()");
                                        exit(EXIT_FAILURE);
                                }
			case 'f':
				if (strlen(optarg) > sizeof(filter)-1) {
					printf("filter name too long\n");
					exit(EXIT_FAILURE);
				}

				strncpy(filter, optarg, sizeof(filter)-1);
				filter[sizeof(filter)-1] = '\0';
				break;
                        case 'h':
                                if (sscanf(optarg, "%d", &maxrcvmsg) != 1 ||
                                        maxrcvmsg < 0) {
                                        printf("wrong -h parameter\n");
                                        exit(EXIT_FAILURE);
                                }
                                break;
			case 's':
				havepath++;
				strncpy(path, optarg, sizeof(path)-1);
				path[sizeof(path)-1] = '\0';
				break;
		}

	if (!havepath) {
		usage(argv[0]);
		exit(EXIT_SUCCESS);	
	}

	if ((buf = (char *)malloc(bsize)) == NULL) {
		fprintf(stderr, "error allocating %ld bytes with malloc(): %s", 
			bsize, strerror(errno));
		exit(EXIT_FAILURE);
	}

        if ((context = zmq_ctx_new()) == NULL) {
                fprintf(stderr, "zmq_ctx_new(): %s", strerror(errno));
                exit(EXIT_FAILURE);
        }

        if ((subscriber = zmq_socket(context, ZMQ_SUB)) == NULL) {
                fprintf(stderr, "zmq_socket(): %s", strerror(errno));
		goto endcontext;
        }

        if (zmq_connect(subscriber, path) < 0) {
                fprintf(stderr, "zmq_connect(): %s", strerror(errno));
		goto endsubscriber;
        }

	if (zmq_setsockopt(subscriber, ZMQ_SUBSCRIBE, filter,
	 strlen(filter)) < 0) {
		fprintf(stderr, "zmq_setsocktopt(ZMQ_SUBSRIBE): %s", 
			strerror(errno));
		goto endsubscriber;
	}

	action.sa_handler = catchsig;
	action.sa_flags = 0;
	sigemptyset(&action.sa_mask);
	sigaction(SIGINT, &action, NULL);

	if (zmq_setsockopt(subscriber, ZMQ_RCVHWM, &maxrcvmsg, sizeof(int)) < 0)
	{ 
		fprintf(stderr, "zmq_setsockopt(ZMQ_RCVHWM): %s", 
			strerror(errno));
		goto endsubscriber;
	} 

        while (!done) {
                while ((n = zmq_recv(subscriber, buf, bsize-1, 0)) < 0) {
                        if (errno == EINTR && !done) 
				continue;	

			goto endsubscriber;
		}

		if (n == bsize-1)
			buf[n++] = '\0';

		write(STDOUT_FILENO, buf, n);
        }

endsubscriber:

	if (zmq_close(subscriber) < 0) {
                fprintf(stderr, "zmq_close(): %s", strerror(errno));
                exit(EXIT_FAILURE);
        }

endcontext:

        if (zmq_ctx_destroy(context) < 0) {
                fprintf(stderr, "zmq_ctx_destroy(): %s", strerror(errno));
                exit(EXIT_FAILURE);
        }

        return (done ? EXIT_SUCCESS : EXIT_FAILURE);
}
