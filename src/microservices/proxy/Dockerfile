FROM golang:1.27.1-alpine3.24 AS builder

WORKDIR /app

COPY go.mod ./
COPY *.go ./
RUN CGO_ENABLED=0 go build -o proxy-service .

FROM alpine:3.24.2

COPY --from=builder /app/proxy-service /usr/local/bin/proxy-service

EXPOSE 8000

CMD ["proxy-service"]
