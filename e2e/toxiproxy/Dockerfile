# Network fault injection for the zombie tests (ghcr.io/shopify/toxiproxy is
# not pullable anonymously here, and Docker Hub only has an old amd64 build).
FROM golang:1.25-alpine AS build
ARG TOXIPROXY_REF=v2.12.0
RUN CGO_ENABLED=0 go install github.com/Shopify/toxiproxy/v2/cmd/server@${TOXIPROXY_REF} && \
    CGO_ENABLED=0 go install github.com/Shopify/toxiproxy/v2/cmd/cli@${TOXIPROXY_REF}

FROM alpine:3.22
COPY --from=build /go/bin/server /usr/bin/toxiproxy
COPY --from=build /go/bin/cli /usr/bin/toxiproxy-cli
EXPOSE 8474
ENTRYPOINT ["/usr/bin/toxiproxy"]
CMD ["-host", "0.0.0.0", "-config", "/etc/toxiproxy.json"]
