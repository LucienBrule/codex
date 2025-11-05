ARG FEDORA_BASE_IMAGE=codex/fedora42-base:dev
FROM ${FEDORA_BASE_IMAGE}

LABEL org.opencontainers.image.title="Codex Fedora 42 NATS" \
      org.opencontainers.image.description="Fedora 42 NATS JetStream broker image for Codex mailbox harness" \
      org.opencontainers.image.vendor="Codex" \
      org.opencontainers.image.version="42" \
      org.opencontainers.image.licenses="Apache-2.0"

ARG NATS_VERSION=2.12.1
ARG NATS_SHA256=a104163114e292f90233afd8cf2bd745d2f04d53948db987ad018735449a2232
ARG NATS_TARBALL="nats-server-v${NATS_VERSION}-linux-amd64.tar.gz"
ARG NATS_DOWNLOAD_URL="https://github.com/nats-io/nats-server/releases/download/v${NATS_VERSION}/${NATS_TARBALL}"
ARG DNF_FLAGS="--setopt=install_weak_deps=False --setopt=tsflags=nodocs"

ENV NATS_SERVER_VERSION="${NATS_VERSION}"
ENV NATS_JETSTREAM_DIR="/jetstream"

USER root

RUN set -euxo pipefail \
    && dnf ${DNF_FLAGS} -y install util-linux-2.40.4-7.fc42.x86_64 \
    && dnf clean all \
    && rm -rf /var/cache/dnf

RUN set -euxo pipefail \
    && curl -sSfL "${NATS_DOWNLOAD_URL}" -o "/tmp/${NATS_TARBALL}" \
    && echo "${NATS_SHA256}  /tmp/${NATS_TARBALL}" | sha256sum -c - \
    && tar -xzf "/tmp/${NATS_TARBALL}" -C /tmp \
    && install -m 0755 /tmp/nats-server-v${NATS_VERSION}-linux-amd64/nats-server /usr/local/bin/nats-server \
    && install -d -m 0755 /opt/codex \
    && mv /tmp/nats-server-v${NATS_VERSION}-linux-amd64/README.md /opt/codex/README.nats-server-${NATS_VERSION}.md \
    && rm -rf /tmp/nats-server-v${NATS_VERSION}-linux-amd64 "/tmp/${NATS_TARBALL}"

COPY entrypoint.sh /usr/local/bin/nats-entrypoint
RUN chmod 0755 /usr/local/bin/nats-entrypoint

VOLUME ["/jetstream"]
EXPOSE 4222 6222 8222

HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=5 \
  CMD bash -c 'curl -fsS "http://127.0.0.1:${NATS_HTTP_PORT:-8222}/healthz" >/dev/null || exit 1'

ENTRYPOINT ["/usr/local/bin/nats-entrypoint"]
CMD ["nats-server"]
