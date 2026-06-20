#!/usr/bin/env sh
# Brightgate Support Tunnel
# Joins the Brightgate tailnet as a userspace node and exposes ONLY Home Assistant
# via Tailscale Serve. No subnet routes, no exit node, no DNS. Off unless started.
set -eu

CONFIG=/data/options.json
AUTHKEY="$(jq -r '.authkey // ""' "$CONFIG")"
HOSTNAME="$(jq -r '.hostname // "brightgate-ha-support"' "$CONFIG")"
HA_URL="$(jq -r '.ha_url // "http://homeassistant:8123"' "$CONFIG")"

export TS_STATE_DIR=/data/tailscale
SOCK=/var/run/tailscale/tailscaled.sock
mkdir -p "$TS_STATE_DIR" /var/run/tailscale

echo "[brightgate] starting tailscaled (userspace) ..."
tailscaled \
  --statedir="$TS_STATE_DIR" \
  --socket="$SOCK" \
  --tun=userspace-networking &
TSD_PID=$!

# Wait for the daemon socket
i=0
while [ ! -S "$SOCK" ] && [ "$i" -lt 30 ]; do
  sleep 1
  i=$((i + 1))
done
if [ ! -S "$SOCK" ]; then
  echo "[brightgate] ERROR: tailscaled socket never appeared" >&2
  exit 1
fi

echo "[brightgate] bringing node up as '${HOSTNAME}' ..."
if [ -n "$AUTHKEY" ]; then
  tailscale up \
    --authkey="$AUTHKEY" \
    --hostname="$HOSTNAME" \
    --accept-routes=false \
    --accept-dns=false \
    --ssh=false
else
  echo "[brightgate] No authkey set in add-on options."
  echo "[brightgate] Bringing up interactively - watch this log for the login URL,"
  echo "[brightgate] approve into the support@brightgatesolutions.com.au tailnet."
  tailscale up \
    --hostname="$HOSTNAME" \
    --accept-routes=false \
    --accept-dns=false \
    --ssh=false
fi

echo "[brightgate] exposing Home Assistant (${HA_URL}) via Tailscale Serve ..."
# Reset any prior serve config, then publish HA on tailnet HTTPS only.
tailscale serve reset || true
tailscale serve --bg --https=443 "$HA_URL"
tailscale serve status || true

echo "[brightgate] support tunnel is up. Stopping the add-on tears it down."
wait "$TSD_PID"
