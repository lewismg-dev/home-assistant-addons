ARG BUILD_FROM
FROM ${BUILD_FROM}

# Tailscale (userspace) + jq for reading add-on options
RUN apk add --no-cache tailscale jq ca-certificates

COPY run.sh /run.sh
RUN chmod a+x /run.sh

CMD [ "/run.sh" ]
