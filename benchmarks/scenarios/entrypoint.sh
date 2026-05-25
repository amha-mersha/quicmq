#!/bin/sh
# entrypoint.sh — applies tc-netem network degradation, then exec's the program.
#
# Environment variables (all optional; 0 / empty = no effect):
#   NETEM_DELAY_MS   — one-way delay in milliseconds (e.g. 50)
#   NETEM_JITTER_MS  — jitter in milliseconds added to delay (e.g. 10)
#   NETEM_LOSS_PCT   — packet-loss probability in percent (e.g. 5)
#   NETEM_RATE_KBIT  — bandwidth cap in kbit/s (e.g. 1000 = 1 Mbit/s)
#   NETEM_CORRUPT_PCT— bit-corruption probability in percent (e.g. 1)
#   NETEM_REORDER_PCT— packet-reorder probability in percent (e.g. 10)
#
# The rules are applied to the container's eth0 outgoing queue, which simulates
# a client on a degraded network.  Requires NET_ADMIN capability.
set -e

DELAY="${NETEM_DELAY_MS:-0}"
JITTER="${NETEM_JITTER_MS:-0}"
LOSS="${NETEM_LOSS_PCT:-0}"
RATE="${NETEM_RATE_KBIT:-0}"
CORRUPT="${NETEM_CORRUPT_PCT:-0}"
REORDER="${NETEM_REORDER_PCT:-0}"

apply_netem() {
    ARGS="tc qdisc add dev eth0 root netem"
    ACTIVE=""

    if [ "$DELAY" != "0" ] && [ "$DELAY" != "" ]; then
        if [ "$JITTER" != "0" ] && [ "$JITTER" != "" ]; then
            ARGS="$ARGS delay ${DELAY}ms ${JITTER}ms distribution normal"
        else
            ARGS="$ARGS delay ${DELAY}ms"
        fi
        ACTIVE="delay=${DELAY}ms jitter=${JITTER}ms $ACTIVE"
    fi

    if [ "$LOSS" != "0" ] && [ "$LOSS" != "" ]; then
        ARGS="$ARGS loss ${LOSS}%"
        ACTIVE="loss=${LOSS}% $ACTIVE"
    fi

    if [ "$CORRUPT" != "0" ] && [ "$CORRUPT" != "" ]; then
        ARGS="$ARGS corrupt ${CORRUPT}%"
        ACTIVE="corrupt=${CORRUPT}% $ACTIVE"
    fi

    if [ "$REORDER" != "0" ] && [ "$REORDER" != "" ]; then
        ARGS="$ARGS reorder ${REORDER}% 25%"
        ACTIVE="reorder=${REORDER}% $ACTIVE"
    fi

    if [ "$RATE" != "0" ] && [ "$RATE" != "" ]; then
        ARGS="$ARGS rate ${RATE}kbit"
        ACTIVE="rate=${RATE}kbit $ACTIVE"
    fi

    if [ -n "$ACTIVE" ]; then
        printf '[netem] applying: %s\n' "$ACTIVE" >&2
        eval "$ARGS" 2>/dev/null && return 0
        printf '[netem] WARNING: tc netem unavailable (need NET_ADMIN cap)\n' >&2
    fi
}

apply_netem

exec "$@"
