# MinIO stopped publishing images (minio/minio and quay.io/minio/minio are
# gone), and its README now says to build from source. This does that.
FROM golang:1.25-alpine AS build
ARG MINIO_REF=latest
RUN CGO_ENABLED=0 go install -trimpath github.com/minio/minio@${MINIO_REF}

FROM alpine:3.22
RUN apk add --no-cache ca-certificates
COPY --from=build /go/bin/minio /usr/bin/minio
VOLUME ["/data"]
EXPOSE 9000 9001
ENTRYPOINT ["/usr/bin/minio"]
