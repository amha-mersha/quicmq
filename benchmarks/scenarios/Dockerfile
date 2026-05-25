# Build context: repository root (quicmq/)
# docker build -f benchmarks/scenarios/Dockerfile -t quicmq-scenarios .

# ── Stage 1: compile all scenario programs ───────────────────────────────────
FROM golang:1.25-alpine AS builder

WORKDIR /build
# Cache module downloads separately from source
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /bin/qmq-pub   ./benchmarks/scenarios/cmd/pub  && \
    CGO_ENABLED=0 go build -ldflags="-s -w" -o /bin/qmq-sub   ./benchmarks/scenarios/cmd/sub  && \
    CGO_ENABLED=0 go build -ldflags="-s -w" -o /bin/qmq-rep   ./benchmarks/scenarios/cmd/rep  && \
    CGO_ENABLED=0 go build -ldflags="-s -w" -o /bin/qmq-req   ./benchmarks/scenarios/cmd/req  && \
    CGO_ENABLED=0 go build -ldflags="-s -w" -o /bin/qmq-dpub  ./benchmarks/scenarios/cmd/dpub && \
    CGO_ENABLED=0 go build -ldflags="-s -w" -o /bin/qmq-dsub  ./benchmarks/scenarios/cmd/dsub

# ── Stage 2: minimal runtime image ncy────────────────────────────────────────────
FROM alpine:3.19

# iproute2 provides tc(8) for netem; jq for JSON post-processing in scripts
RUN apk add --no-cache iproute2 bash jq

COPY --from=builder /bin/qmq-* /bin/
COPY benchmarks/scenarios/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
