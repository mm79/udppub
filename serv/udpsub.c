/*
 * cc udppub.c -o udppub -lzmq
 *
 * This daemon receives udp packets and send on a socket of type ZMQ_PUB 
 * 
 * According to zmq_socket(3):
 *
 * "A socket of type ZMQ_PUB is used by a publisher to distribute data. 
 * Messages sent are distributed in a fan out fashion to all connected peers. 
 *
 * When a ZMQ_PUB socket enters an exceptional state due to having reached the 
 * high water mark for a subscriber, then  any messages that would be sent to 
 * the subscriber in question shall be dropped until the exceptional state 
 * ends.
 * 
 * The ZMQ_SNDHWM option shall set the high water mark for outbound messages on 
 * the specified socket. 
 * The high water mark is a hard limit on the maximum number of outstanding 
 * messages 0MQ shall queue in memory for any single peer that the specified
 * socket is communicating with.
 *
 * If this limit has been reached the socket shall enter an exceptional state 
 * and depending on the socket type, 
 * 0mq shall take appropriate action such as blocking or dropping sent messages 
 * Refer to the individual socket descriptions in zmq_socket(3) for details on 
 * the exact action taken for each socket type.
 *
 * The default ZMQ_SNDHWM value of zero means "no limit".
 *
 * Tested on LibZMQ3
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
#include <syslog.h>
#include <unistd.h>
#include <limits.h>
#include <grp.h>
#include <pwd.h>
#include <errno.h>

#include <zmq.h>
        
int daemonize = 1;
int rcvlen = 65536;
int debug;
int done;

static void 
usage(const char *name)
{
	printf(
		"\n%s\n"
		"\t-b bsize 	buffer size for recvfrom() from udp socket\n"
		"\t-d 		for debug purposes\n"
		"\t-g groupname drop privileges to group (require also -u)\n"
		"\t-h messages	set ZMQ_SNDHWM default 0 (unlimited)\n"
		"\t-n 		do not call daemon()\n"		
		"\t-p port	udp port\n"
		"\t-r size	set SO_RCVBUF for udp socket (default 65536)\n"
		"\t-s pathname	path name eg. ipc:///tmp/serv.ipc\n"
		"\t-u username	drop privileges to user (require also -g)\n\n"
		"example: %s -b 508 -p 40000 -s ipc:///tmp/serv.ipc\n\n",	
		name, name);	
}

static void 
drop_priv(uid_t uid, gid_t gid)
{
        /* drop privileges */
        if (getuid() == 0) {
                if (setgroups(1, &gid) == -1) {
			perror("setgroups()");
			exit(1);
		}
                if (setgid(gid) == -1) {
			perror("setgid()");
                        exit(1);
                }
		if (setuid(uid) == -1) {
			perror("setuid()");
                        exit(1);
                }
        }

	if (setuid(0) != -1) {
		fprintf(stderr, "unable to drop privileges"); 
                exit(1);
        }
}

static void
catchsig()
{
	done = 1;
}

static void
msg(int prio, const char *fmt, ...)
{
	char msg[8192];
	va_list vl;

	va_start(vl, fmt);
	vsnprintf(msg, sizeof(msg), fmt, vl);

	if (daemonize && !debug)
		syslog(prio, "%s", msg);
	else
		printf("%s\n", msg);

	va_end(vl);
}

int
main (int argc, char *argv[])
{
	char path[PATH_MAX] = "\0";
	struct sigaction action;
        struct sockaddr_in sin, peer_sa;
        socklen_t salen;
	struct passwd *pw;
	struct group *gr;
        char *buf, *dropuser = NULL, *dropgroup = NULL;
	void *context, *publisher;
        int fd, n, opt;
	int havepath = 0;
	int maxsndmsg = 0;
	size_t bsize = 1500;
	uint16_t port = 0;

	extern char *optarg;


	while ((opt = getopt(argc, argv, "b:dg:h:np:r:s:u:")) != -1)
		switch (opt) {
			case 'b':
				if (sscanf(optarg, "%zd", &bsize) != 1) {
					perror("sscanf()");
					exit(1);
				}

				/*
				 * limit buffer size
				 */
				if (bsize <= 0 || bsize > (1<<16)) {
					printf("wrong bsize parameter\n");
					exit(1);
				}
				break;
			case 'd':
				debug++;
				break;
			case 'g':
				dropgroup = optarg;
				break;
			case 'h':
				if (sscanf(optarg, "%d", &maxsndmsg) != 1 ||
					maxsndmsg < 0) {
					printf("wrong -h parameter\n");
					exit(1);
				}
				break;
			case 'n':
				daemonize = 0;	
				break;
			case 'p':
				if (sscanf(optarg, "%hu", &port) != 1) {
					printf("wrong -p parameter\n");
					exit(1);
				} 
				break;
			case 'r':
				if (sscanf(optarg, "%d", &rcvlen) != 1 ||
					rcvlen < 0) {
					printf("wrong -r parameter\n");
					exit(1);
				}
				break;
				break;
			case 's':
				havepath++;
				strncpy(path, optarg, sizeof(path)-1);
				path[sizeof(path)-1] = '\0';
				break;
			case 'u':
				dropuser = optarg;
				break;

		}

	if (port == 0 || *path == '\0' ||
		(dropuser != NULL && dropgroup == NULL) ||
		(dropgroup == NULL && dropuser != NULL)) {
		usage(argv[0]);
		exit(0);
	}

	if (dropuser && dropgroup) {
		if ((pw = getpwnam(dropuser)) == NULL) {
			fprintf(stderr, "Unable to find user %s\n", dropuser);
			exit(1);
		}

		if ((gr = getgrnam(dropgroup)) == NULL) {
			fprintf(stderr, "Unable to find group %s\n", dropgroup);
			exit(1);
		}

		drop_priv(pw->pw_uid, gr->gr_gid);
	}

	if (daemonize && !debug) {
		if (daemon(0, 0) < 0) {
			perror("daemon()");
			exit(1);
		}

		openlog("udppub", LOG_PID | LOG_NDELAY, LOG_DAEMON);
	}

	if ((buf = (char *)malloc(bsize)) == NULL) {
		msg(LOG_ERR, "error allocating %d bytes with malloc(): %s", 
			bsize, strerror(errno));
		exit(1);
	}

        if ((context = zmq_ctx_new()) == NULL) {
                msg(LOG_ERR, "zmq_ctx_new(): %s", strerror(errno));
                exit(1);
        }

        if ((publisher = zmq_socket(context, ZMQ_PUB)) == NULL) {
                msg(LOG_ERR, "zmq_socket(): %s", strerror(errno));
		zmq_ctx_destroy(context);

                exit(1);
        }

	action.sa_handler = catchsig;
	action.sa_flags = 0;
	sigemptyset(&action.sa_mask);
	sigaction(SIGINT, &action, NULL);
	sigaction(SIGTERM, &action, NULL);

	/*
	 * setting high water mark for outbound messages
 	 * note: messages are dropped if this limit is reached
	 * default value depends from version (1000 or 0)
	 * where 0 = unlimited
	 * we set "unlimited" by default
	 *
	 * note: "0MQ does not guarantee that the socket will accept as many as
	 * ZMQ_SNDHWM messages, and the actual limit may be as much as 60-70% 
	 * lower depending on the flow of messages on the socket."
	 */
	if (zmq_setsockopt(publisher, ZMQ_SNDHWM, &maxsndmsg, sizeof(int)) < 0)
	{ 
		msg(LOG_ERR, "zmq_setsockopt(ZMQ_SNDHWM): %s", strerror(errno));
		exit(1);
	} 

        if (zmq_bind(publisher, path) < 0) {
                msg(LOG_ERR, "zmq_bind(): %s", strerror(errno));
                exit(1);
        }

        memset(&sin, 0, sizeof sin);
        sin.sin_family = AF_INET;
        sin.sin_port = htons(port);
        sin.sin_addr.s_addr = htonl(INADDR_ANY);

        if ((fd = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)) < 0) {
                msg(LOG_ERR, "udp socket(): %s", strerror(errno));
                exit(1);
        }

	if (setsockopt(fd, SOL_SOCKET, SO_RCVBUF, (char *)&rcvlen, sizeof rcvlen) < 0)
	{
        	msg(LOG_ERR, "error setting so_rcvbuf = %d: %s", rcvlen, 
			strerror(errno));
         	exit(1);
        }

        if (bind(fd, (struct sockaddr *)&sin, sizeof sin) < 0) {
                msg(LOG_ERR, "udp bind(): %s", strerror(errno));
                exit(1);
        }

        while (!done) {
                salen = sizeof(peer_sa);

                if ((n = recvfrom(fd, (void *)buf, bsize-1, 0,
                        (struct sockaddr *)&peer_sa, &salen)) <= 0)
                        continue;

                buf[n++] = '\0';

		/*
		 * do not expect good performance with debug enabled 
		 */
		if (debug)
			msg(LOG_DEBUG, "%s", buf);

                /*
                 * checking if zmq_send() was interrupted by a delivery of a 
		 * signal before the message was sent
		 *
		 * note: The zmq_send() function shall never block for this 
		 * socket type
                 */
                while (zmq_send(publisher, buf, n, 0) < 0) {
                        if (errno == EINTR && !done) 
				continue;	

			if (debug)
				msg(LOG_DEBUG, 
				  "error sending %d bytes with zmq_send(): %s",
				  n, strerror(errno));
			break;
		}
        }

	if (zmq_close(publisher) < 0) {
                msg(LOG_ERR, "zmq_close(): %s", strerror(errno));
                exit(1);
        }

        if (zmq_ctx_destroy(context) < 0) {
                msg(LOG_ERR, "zmq_ctx_destroy(): %s", strerror(errno));
                exit(1);
        }

        return 0;
}
