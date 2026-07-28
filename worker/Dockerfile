FROM golang:alpine AS builder
WORKDIR /app

COPY . .
RUN go mod download

RUN go build -o main ./cmd/main.go



FROM alpine:latest
WORKDIR /app

COPY --from=builder /app/main .

RUN chmod +x main
CMD ["./main"]