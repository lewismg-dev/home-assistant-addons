#!/usr/bin/env sh
# Brightgate Connector entrypoint.
# Starts tailscaled (userspace) in the background but does NOT bring the node up.
# The Python service (app.py) brings Tailscale up + publishes Serve only while a
# support session is granted, and tears it down on revoke/expiry.
set -eu

export TS_STATE_DIR=/data/tailscale
export TS_SOCKET=/var/run/tailscale/tailscaled.sock
mkdir -p "$TS_STATE_DIR" /var/run/tailscale

echo "[brightgate] starting tailscaled (userspace, idle until a session is granted) ..."
tailscaled \
  --statedir="$TS_STATE_DIR" \
  --socket="$TS_SOCKET" \
  --tun=userspace-networking &

# Hand off to the connector service. It owns enrollment, the Ingress UI, the
# heartbeat loop, and session/tunnel control.
exec python3 /app.py
