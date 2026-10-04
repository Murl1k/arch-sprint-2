FROM golang:1.27.1-alpine3.24 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./
RUN CGO_ENABLED=0 go build -o events-service .

FROM alpine:3.24.2

COPY --from=builder /app/events-service /usr/local/bin/events-service

EXPOSE 8082

CMD ["events-service"]
