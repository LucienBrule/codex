ARG FEDORA_BASE_IMAGE=codex/fedora42-base:dev
FROM ${FEDORA_BASE_IMAGE}

LABEL org.opencontainers.image.title="Codex Fedora 42 Mail Server" \
      org.opencontainers.image.description="Fedora 42 container image bundling the codex-mail-server dispatcher" \
      org.opencontainers.image.vendor="Codex" \
      org.opencontainers.image.version="42" \
      org.opencontainers.image.licenses="Apache-2.0"

ARG CODEX_MAIL_SERVER_VERSION=dev

ENV CODEX_MAIL_SERVER_VERSION="${CODEX_MAIL_SERVER_VERSION}" \
    CODEX_MAIL_SERVER_BIN="/usr/local/bin/codex-mail-server" \
    CODEX_MAIL_SERVER_RUNTIME_DIR="/var/run/codex-mailbox" \
    CODEX_MAIL_SERVER_LOG_DIR="/var/log/codex-mailbox"

USER root

ARG DNF_FLAGS="--setopt=install_weak_deps=False --setopt=tsflags=nodocs"
RUN dnf ${DNF_FLAGS} -y install \
        util-linux-2.40.4-7.fc42.x86_64 \
    && dnf clean all \
    && rm -rf /var/cache/dnf

# The build expects a promoted codex-mail-server binary to be staged in this
# directory prior to docker build (see make mailbox-mail-server-image).
COPY codex-mail-server "${CODEX_MAIL_SERVER_BIN}"
RUN chmod 0755 "${CODEX_MAIL_SERVER_BIN}"

COPY entrypoint.sh /usr/local/bin/codex-mail-server-entrypoint
RUN chmod 0755 /usr/local/bin/codex-mail-server-entrypoint

COPY config /opt/codex/mail-server/config

RUN set -euxo pipefail \
    && install -d -m 0770 -o codex -g codex "${CODEX_MAIL_SERVER_RUNTIME_DIR}" \
    && install -d -m 0770 -o codex -g codex "${CODEX_MAIL_SERVER_LOG_DIR}" \
    && install -d -m 0755 -o codex -g codex /opt/codex/mail-server

VOLUME ["/var/run/codex-mailbox", "/var/log/codex-mailbox"]

ENTRYPOINT ["/usr/local/bin/codex-mail-server-entrypoint"]
CMD ["codex-mail-server"]
