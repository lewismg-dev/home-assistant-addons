ARG BUILD_FROM
FROM ${BUILD_FROM}

# tailscale: the tunnel. python3 + aiohttp: the connector service (Ingress UI,
# enrollment, heartbeat, session/tunnel control). jq: parse tailscale status.
RUN apk add --no-cache \
      tailscale \
      jq \
      ca-certificates \
      python3 \
      py3-aiohttp

COPY run.sh /run.sh
COPY app.py /app.py
RUN chmod a+x /run.sh

CMD [ "/run.sh" ]
