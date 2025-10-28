GIT_HASH := $(shell git rev-parse HEAD)
FEDORA_BASE_IMAGE := codex/fedora42-base:$(GIT_HASH)
FEDORA_BASE_CONTEXT := docker/fedora42-base
FEDORA_NATS_IMAGE := codex/fedora42-nats:$(GIT_HASH)
FEDORA_NATS_CONTEXT := docker/fedora42-nats
FEDORA_MAIL_SERVER_IMAGE := codex/fedora42-mail-server:$(GIT_HASH)
FEDORA_MAIL_SERVER_CONTEXT := docker/fedora42-mail-server

MAIL_SERVER_BINARY ?= codex-mail-server/target/release/codex-mail-server
MAIL_SERVER_VERSION ?= $(GIT_HASH)

.PHONY: mailbox-docker-base
mailbox-docker-base:
	@echo "[codex] Building Fedora 42 base image ($(FEDORA_BASE_IMAGE))"
	docker build \
		--file $(FEDORA_BASE_CONTEXT)/Dockerfile \
		--tag $(FEDORA_BASE_IMAGE) \
		--tag codex/fedora42-base:dev \
		$(FEDORA_BASE_CONTEXT)

.PHONY: mailbox-docker-nats
mailbox-docker-nats: mailbox-docker-base
	@echo "[codex] Building Fedora 42 NATS image ($(FEDORA_NATS_IMAGE))"
	docker build \
		--file $(FEDORA_NATS_CONTEXT)/Dockerfile \
		--build-arg FEDORA_BASE_IMAGE=$(FEDORA_BASE_IMAGE) \
		--tag $(FEDORA_NATS_IMAGE) \
		--tag codex/fedora42-nats:dev \
		$(FEDORA_NATS_CONTEXT)

.PHONY: mailbox-mail-server-image
mailbox-mail-server-image: mailbox-docker-base
	@if [ ! -f "$(MAIL_SERVER_BINARY)" ]; then \
	  echo "[codex] mail server binary not found at $(MAIL_SERVER_BINARY)"; \
	  echo "Hint: run cargo build --release -p codex-mail-server or set MAIL_SERVER_BINARY=/path/to/codex-mail-server"; \
	  exit 2; \
	fi
	@TMP="$(FEDORA_MAIL_SERVER_CONTEXT)/codex-mail-server"; \
	echo "[codex] Building Fedora 42 mail server image ($(FEDORA_MAIL_SERVER_IMAGE))"; \
	install -m 0755 "$(MAIL_SERVER_BINARY)" "$$TMP"; \
	docker build \
		--file $(FEDORA_MAIL_SERVER_CONTEXT)/Dockerfile \
		--build-arg FEDORA_BASE_IMAGE=$(FEDORA_BASE_IMAGE) \
		--build-arg CODEX_MAIL_SERVER_VERSION=$(MAIL_SERVER_VERSION) \
		--tag $(FEDORA_MAIL_SERVER_IMAGE) \
		--tag codex/fedora42-mail-server:dev \
		$(FEDORA_MAIL_SERVER_CONTEXT); \
	status=$$?; \
	rm -f "$$TMP"; \
	exit $$status
