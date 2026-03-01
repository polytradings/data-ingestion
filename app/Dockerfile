FROM golang:1.22-alpine AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG SERVICE=crypto-ingestion
RUN CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o /out/service ./cmd/${SERVICE}

FROM alpine:3.20
WORKDIR /app
RUN addgroup -S app && adduser -S app -G app
COPY --from=builder /out/service /app/service
USER app
ENTRYPOINT ["/app/service"]
