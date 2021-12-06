# we need the same debian version on the rust and varnish so
# that libssl-dev and libssl3 match
FROM rust:1.83-bookworm

WORKDIR /vmod_prequal
ARG VMOD_prequal_VERSION=0.0.1
ARG RELEASE_URL=https://github.com/gquintard/vmod_prequal/archive/refs/tags/v${VMOD_prequal_VERSION}.tar.gz
ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse

RUN set -e; \
	curl -s https://packagecloud.io/install/repositories/varnishcache/varnish76/script.deb.sh | bash; \
	apt-get install -y varnish-dev clang libssl-dev; \
	curl -Lo dist.tar.gz ${RELEASE_URL}; \
	tar xavf dist.tar.gz --strip-components=1; \
  cargo tree -d; \
	cargo build --release

FROM varnish:7.6
USER root
RUN set -e; \
	apt-get update; \
	apt-get install -y libssl3; \
	rm -rf /var/lib/apt/lists/*
COPY --from=0 /vmod_prequal/target/release/libvmod_prequal.so /usr/lib/varnish/vmods/
USER varnish
