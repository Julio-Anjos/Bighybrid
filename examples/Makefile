CC = g++ -std=c++14
CFLAGS = -Wall -g3
#CFLAGS = -Wall -lpthread -g3 -O3

INSTALL_PATH = /opt/simgrid
INCLUDES = -I../include -I$(INSTALL_PATH)/include
DEFS = -L$(INSTALL_PATH)/lib64
LDADD = -lm -lsimgrid

SRC=$(wildcard *.cpp)
BIN=$(SRC:.c=.bin)

#all: $(BIN)
all: hello_bighybrid.bin

#%.bin: %.cpp
#	$(CC) $(INCLUDES) $(DEFS) -o $@ $^ ../libbighybrid.a $(LDADD)

hello_bighybrid.bin: hello_bighybrid.cpp
	$(CC) $(INCLUDES) $(DEFS) -o $@ $^ ../libbighybrid.a $(LDADD)

clean:
	rm -vf *.bin *.trace *.plist *.log *.csv

.PHONY: clean
