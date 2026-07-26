#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <syslog.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <pthread.h>
#include <time.h>

#define PORT     "9000"
#define DATAFILE "/var/tmp/aesdsocketdata"
#define BACKLOG  10
#define BUFSIZE  1024

static int server_fd = -1;
static int client_fd = -1;
static volatile sig_atomic_t caught_signal = 0;
static pthread_mutex_t data_mux = PTHREAD_MUTEX_INITIALIZER;

typedef struct threadNode_t
{
    pthread_t threadId;
    int clientFd;
    char clientIp[INET6_ADDRSTRLEN];
    int finished;
    struct threadNode_t *next;
} threadNode_t;
static threadNode_t *head = NULL;

static void signal_handler(int signo)
{
    (void)signo;
    caught_signal = 1;
}

static void cleanup(void)
{
    if (client_fd != -1) {
        close(client_fd);
        client_fd = -1;
    }
    if (server_fd != -1) {
        close(server_fd);
        server_fd = -1;
    }
    remove(DATAFILE);
    threadNode_t **p = &head;
    while( *p )
    {
        threadNode_t *n = *p;
        if( n->finished )
        {
            pthread_join( n->threadId, NULL );
            *p = n->next;
            free( n );
            continue;
        }
        p = &n->next;
    }
    closelog();
}

static int setup_signals(void)
{
    /* Setup signal handler for interrupt and terminate */
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    if (sigaction(SIGINT,  &sa, NULL) == -1) return -1;
    if (sigaction(SIGTERM, &sa, NULL) == -1) return -1;
    return 0;
}

/* Send the full contents of DATAFILE to fd. */
static int send_file(int fd)
{
    FILE *fp = fopen(DATAFILE, "r");
    if (!fp) {
        syslog(LOG_ERR, "fopen %s for read failed: %m", DATAFILE);
        return -1;
    }
    char buf[BUFSIZE];
    size_t n;

    while ((n = fread(buf, 1, sizeof(buf), fp)) > 0) {
        size_t sent = 0;
        /* Ensure full BUFSIZE is sent in case of short write */
        while (sent < n) {
            ssize_t s = send(fd, buf + sent, n - sent, 0);
            if (s == -1) {
                fclose(fp);
                return -1;
            }
            sent += (size_t)s;
        }
    }
    fclose(fp);
    return 0;
}

/* Handle one accepted client connection. */
static void *handle_client( void* arg )
{
    threadNode_t *curr_node = ( threadNode_t *)arg;
    int fd = curr_node->clientFd;
    const char* client_ip = curr_node->clientIp;

    char   *packet     = NULL;
    size_t  packet_len = 0;
    char    buf[BUFSIZE];
    ssize_t nbytes;

    while ((nbytes = recv(fd, buf, sizeof(buf), 0)) > 0) {
        /* Grow packet buffer */
        char *tmp = realloc(packet, packet_len + (size_t)nbytes + 1);
        if (!tmp) {
            syslog(LOG_ERR, "realloc failed: %m");
            break;
        }
        packet = tmp;
        memcpy(packet + packet_len, buf, (size_t)nbytes);
        packet_len += (size_t)nbytes;
        packet[packet_len] = '\0';

        /* Process every complete packet (newline-terminated) in the buffer */
        char *newline;
        while ((newline = memchr(packet, '\n', packet_len)) != NULL) {
            size_t write_len = (size_t)(newline - packet) + 1;

            /* Append packet (up to and including '\n') to data file */
            pthread_mutex_lock( &data_mux );
            FILE *fp = fopen(DATAFILE, "a");
            if (!fp) {
                syslog(LOG_ERR, "fopen %s for append failed: %m", DATAFILE);
                pthread_mutex_unlock( &data_mux );
                break;
            }
            if (fwrite(packet, 1, write_len, fp) != write_len) {
                syslog(LOG_ERR, "fwrite failed: %m");
                fclose(fp);
                pthread_mutex_unlock( &data_mux );
                break;
            }
            fclose(fp);

            /* Send full file back to client */
            if (send_file(fd) == -1)
                syslog(LOG_ERR, "send_file failed for %s", client_ip);
            pthread_mutex_unlock( &data_mux );

            size_t remaining = packet_len - write_len;
            if (remaining > 0)
                /* In case there is data in received string past 1 new line char */
                memmove(packet, newline + 1, remaining);
            packet_len = remaining;
        }
    }

    if (nbytes == -1 && errno != EINTR)
        syslog(LOG_ERR, "recv error from %s: %m", client_ip);

    free(packet);
    close(curr_node->clientFd);
    curr_node->finished = 1;
    syslog(LOG_INFO, "Closed connection from %s", client_ip);
    return NULL;
}

static void daemonize(void)
{
    pid_t pid = fork();
    if (pid < 0) {
        syslog(LOG_ERR, "fork failed: %m");
        exit(EXIT_FAILURE);
    }
    if (pid > 0)
        exit(EXIT_SUCCESS); /* parent exits */

    if (setsid() == -1) {
        syslog(LOG_ERR, "setsid failed: %m");
        exit(EXIT_FAILURE);
    }

    /* Redirect stdin/stdout/stderr to /dev/null */
    int devnull = open("/dev/null", O_RDWR);
    if (devnull != -1) {
        dup2(devnull, STDIN_FILENO);
        dup2(devnull, STDOUT_FILENO);
        dup2(devnull, STDERR_FILENO);
        if (devnull > STDERR_FILENO)
            close(devnull);
    }
}

static void* timer_thread( void* arg )
{
    (void)arg;
    while( !caught_signal )
    {
        sleep( 10 );
        if( caught_signal )
            break;
        time_t t = time( NULL );
        struct tm tm;
        localtime_r( &t, &tm );
        char timestr[128];
        strftime(timestr, sizeof(timestr), "timestamp:%Y-%m-%d %H:%M:%S\n", &tm);
        if (pthread_mutex_lock(&data_mux) == 0) {
            FILE *fp = fopen(DATAFILE, "a");
            if (fp) {
                fwrite(timestr, 1, strlen(timestr), fp);
                fclose(fp);
            } else {
                syslog(LOG_ERR, "timer fopen %s failed: %m", DATAFILE);
            }
            pthread_mutex_unlock(&data_mux);
        } else {
            syslog(LOG_ERR, "timer pthread_mutex_lock failed: %m");
        }
    }
    return NULL;
} 

int main(int argc, char *argv[])
{
    int daemon_mode = 0;

    if (argc == 2 && strcmp(argv[1], "-d") == 0)
        daemon_mode = 1;

    openlog("aesdsocket", LOG_PID, LOG_USER);

    if (setup_signals() == -1) {
        syslog(LOG_ERR, "sigaction failed: %m");
        return -1;
    }

    /* Resolve bind address */
    struct addrinfo hints, *res, *rp;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags    = AI_PASSIVE;

    int gai_rc = getaddrinfo(NULL, PORT, &hints, &res);
    if (gai_rc != 0) {
        syslog(LOG_ERR, "getaddrinfo: %s", gai_strerror(gai_rc));
        return -1;
    }

    for (rp = res; rp != NULL; rp = rp->ai_next) {
        server_fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (server_fd == -1)
            continue;

        int yes = 1;
        setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

        if (bind(server_fd, rp->ai_addr, rp->ai_addrlen) == 0)
            break;

        close(server_fd);
        server_fd = -1;
    }
    freeaddrinfo(res);

    if (server_fd == -1) {
        syslog(LOG_ERR, "Could not bind to port %s", PORT);
        return -1;
    }

    /* Daemonize AFTER successful bind */
    if (daemon_mode)
        daemonize();

    if (listen(server_fd, BACKLOG) == -1) {
        syslog(LOG_ERR, "listen failed: %m");
        cleanup();
        return -1;
    }
    pthread_t timer_tid;
    if (pthread_create(&timer_tid, NULL, timer_thread, NULL) != 0)
        syslog(LOG_ERR, "Failed to create timer thread: %m");

    /* Accept loop */
    while (!caught_signal) {
        struct sockaddr_storage client_addr;
        socklen_t addrlen = sizeof(client_addr);

        client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &addrlen);
        if (client_fd == -1) {
            if (errno == EINTR)
                break;
            syslog(LOG_ERR, "accept failed: %m");
            continue;
        }

        /* Resolve client IP */
        char client_ip[INET6_ADDRSTRLEN];
        if (client_addr.ss_family == AF_INET) {
            struct sockaddr_in *s = (struct sockaddr_in *)&client_addr;
            inet_ntop(AF_INET, &s->sin_addr, client_ip, sizeof(client_ip));
        } else {
            struct sockaddr_in6 *s = (struct sockaddr_in6 *)&client_addr;
            inet_ntop(AF_INET6, &s->sin6_addr, client_ip, sizeof(client_ip));
        }

        threadNode_t *curr_thread = malloc( sizeof(threadNode_t) );
        curr_thread->clientFd = client_fd;
        strcpy( curr_thread->clientIp, client_ip);
        curr_thread->finished = 0;
        curr_thread->next = head;
        head = curr_thread;

        syslog(LOG_INFO, "Accepted connection from %s", client_ip);
        pthread_create( &curr_thread->threadId, NULL, handle_client, curr_thread );

        threadNode_t **p = &head;
        while( *p )
        {
            threadNode_t *n = *p;
            if( n->finished )
            {
                pthread_join( n->threadId, NULL );
                *p = n->next;
                free( n );
                continue;
            }
            p = &n->next;
        }
    }
    syslog(LOG_INFO, "Caught signal, exiting");
    cleanup();
    pthread_join( timer_tid, NULL );
    return 0;
}
