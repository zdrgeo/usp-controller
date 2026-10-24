FROM golang:1.27.1-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/usp-controller ./cmd/usp-controller

FROM alpine:3.23
RUN apk add --no-cache ca-certificates && mkdir /data && chown 65532:65532 /data
COPY --from=build /out/usp-controller /usr/local/bin/usp-controller
COPY THIRD_PARTY_NOTICES /usr/share/licenses/usp-controller/THIRD_PARTY_NOTICES
USER 65532:65532
WORKDIR /data
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/usp-controller"]
CMD ["-config", "/etc/usp/config.json"]
