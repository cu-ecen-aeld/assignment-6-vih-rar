CROSS_COMPILE ?=
CC = $(CROSS_COMPILE)gcc
CFLAGS = -Wall -Werror -g -pthread -time

TARGET = aesdsocket
SRCS   = aesdsocket.c
OBJS   = $(SRCS:.c=.o)

.PHONY: all default clean

default: all

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^

%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<

clean:
	rm -f $(OBJS) $(TARGET)
