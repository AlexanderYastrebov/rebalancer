FROM golang:1.26 AS builder

WORKDIR /workspace

COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o rebalancer .

FROM scratch

COPY --from=builder /workspace/rebalancer /rebalancer

ENTRYPOINT ["/rebalancer"]
