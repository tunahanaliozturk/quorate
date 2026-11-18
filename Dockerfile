# Builds the quorate binary and puts it in a small image.

FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY . .
# The repository turns warnings into errors. That is right for CI and wrong for an image build,
# which should not fail because a newer compiler has a new lint.
ENV RUSTFLAGS="--cap-lints warn"
RUN cargo build --release --locked --bin quorate

FROM debian:bookworm-slim
RUN useradd --system --create-home --home-dir /var/lib/quorate quorate
COPY --from=build /src/target/release/quorate /usr/local/bin/quorate
USER quorate
VOLUME /var/lib/quorate
EXPOSE 7000
ENTRYPOINT ["quorate"]
