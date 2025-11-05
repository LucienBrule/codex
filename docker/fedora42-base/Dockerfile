ARG FEDORA_IMAGE=registry.fedoraproject.org/fedora:42
FROM ${FEDORA_IMAGE}

LABEL org.opencontainers.image.title="Codex Fedora 42 Base" \
      org.opencontainers.image.description="Base image for Codex mailbox harness workloads" \
      org.opencontainers.image.vendor="Codex" \
      org.opencontainers.image.version="42" \
      org.opencontainers.image.licenses="Apache-2.0"

# Pinned RPM versions captured on 2025-10-25.
ARG DNF_FLAGS="--setopt=install_weak_deps=False --setopt=tsflags=nodocs"
RUN dnf ${DNF_FLAGS} -y install \
        bash-5.2.37-1.fc42.x86_64 \
        ca-certificates-2025.2.80_v9.0.304-1.0.fc42.noarch \
        curl-8.11.1-6.fc42.x86_64 \
        jq-1.7.1-11.fc42.x86_64 \
        tar-1.35-5.fc42.x86_64 \
        tini-0.19.0-10.fc42.x86_64 \
    && dnf clean all \
    && rm -rf /var/cache/dnf

ARG CODEX_UID=1000
ARG CODEX_GID=1000

# Create codex user/group and workspace directories.
RUN groupadd --gid "${CODEX_GID}" codex \
    && useradd --uid "${CODEX_UID}" --gid "${CODEX_GID}" --create-home --home-dir /home/codex codex \
    && install -d -o codex -g codex /workspace

COPY entrypoint.sh /usr/local/bin/codex-entrypoint
RUN chmod 0755 /usr/local/bin/codex-entrypoint

WORKDIR /workspace
USER codex:codex

ENTRYPOINT ["/usr/local/bin/codex-entrypoint"]
CMD ["/bin/bash"]
