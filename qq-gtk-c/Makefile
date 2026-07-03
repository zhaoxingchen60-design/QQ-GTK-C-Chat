CC ?= gcc
PKG_CONFIG ?= pkg-config

CFLAGS ?= -Wall -Wextra -O2 -g
CPPFLAGS += -Isrc
THREAD_LIBS := -pthread
GTK_CFLAGS := $(shell $(PKG_CONFIG) --cflags gtk4)
GTK_LIBS := $(shell $(PKG_CONFIG) --libs gtk4)

BIN_DIR := bin
DATA_DIR := data
DOWNLOAD_DIR := downloads

SERVER := $(BIN_DIR)/qq_server
CLIENT := $(BIN_DIR)/qq_client

.PHONY: all clean dirs check-deps

all: check-deps dirs $(SERVER) $(CLIENT)

check-deps:
	@$(PKG_CONFIG) --exists gtk4 || { \
		echo "gtk4 development package not found."; \
		echo "Install it on openEuler with:"; \
		echo "  sudo dnf install -y gcc make pkgconf-pkg-config gtk4-devel"; \
		exit 1; \
	}

dirs:
	@mkdir -p $(BIN_DIR) $(DATA_DIR) $(DOWNLOAD_DIR)

$(SERVER): src/server.c src/protocol.c src/protocol.h | dirs
	$(CC) $(CPPFLAGS) $(CFLAGS) src/server.c src/protocol.c -o $@ $(THREAD_LIBS) -lrt

$(CLIENT): src/client_gtk4.c src/protocol.c src/protocol.h | dirs
	$(CC) $(CPPFLAGS) $(CFLAGS) $(GTK_CFLAGS) src/client_gtk4.c src/protocol.c -o $@ $(THREAD_LIBS) $(GTK_LIBS)

clean:
	rm -f $(SERVER) $(CLIENT)
