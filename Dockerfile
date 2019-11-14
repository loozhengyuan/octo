# Create builder image
FROM golang:latest AS builder

# Set working directoy
WORKDIR /app

# Install go modules
COPY go.mod go.sum ./
RUN go mod download

# Verify downloaded modules with go.sum
RUN go mod verify

# Build binary
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -installsuffix cgo -o octo .

# Create production image
FROM alpine:latest

# Set working directory
WORKDIR /app

# Enable SSL/TLS
RUN apk --no-cache add ca-certificates

# Copy binary
COPY --from=builder /app .

# Set entrypoint
ENTRYPOINT ["./octo"]