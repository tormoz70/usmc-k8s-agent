FROM golang:1.22-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/k8s-agent ./cmd/agent

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=builder /out/k8s-agent /app/k8s-agent
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/app/k8s-agent"]
