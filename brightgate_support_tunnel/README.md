# Brightgate Connector

Self-enrolling remote-support connector for Brightgate Solutions. It is **off by
default** and, during a granted session, exposes **only Home Assistant** to the
Brightgate tailnet via [Tailscale](https://tailscale.com) Serve. No subnet routes,
no exit node, no DNS — minimal blast radius. Userspace networking, so it never
conflicts with a client's own Tailscale add-on.

This is **vendor-managed infrastructure** — the homeowner controls only the
grant/revoke switch on the add-on's own panel. There are no HA helpers,
automations, `rest_command`s, or editable dashboards to break.

## What it does
1. **Enrollment** — open the add-on's panel and paste the **enrollment code** from
   the Brightgate portal. The connector exchanges it for this client's credentials
   (`client_id`, heartbeat secret, support hostname) and stores them in `/data`.
2. **Heartbeat** — every 10 minutes it reports liveness + basic system health +
   tunnel state to the portal (outbound; works even when support is off).
3. **Grant/revoke** — the homeowner grants time-limited access from the panel. On
   grant it brings Tailscale up and publishes Serve; on revoke or timer expiry it
   tears the tunnel down.

## Options
| Option | Meaning |
|--------|---------|
| `portal_url` | Brightgate portal base URL. Default `https://portal.brightgatesolutions.com.au`. |
| `log_level` | `debug` / `info` / `warning` / `error`. |

No secrets, client IDs, or hostnames are configured by hand — they come from the
portal at enrollment.

## Requirements on the Brightgate tailnet
- MagicDNS + HTTPS certificates **enabled** (for `tailscale serve --https`).
- An ACL granting Brightgate admins access to `tag:ha-support:443` only.
- A portal `POST /api/enroll` endpoint that issues per-client credentials (and,
  optionally, a tagged auth key so first registration is zero-touch).

## Per-machine install
Add the Brightgate add-on repository once, install **Brightgate Connector**, open
its panel, paste the enrollment code. Done — no YAML, no per-client file edits.
The only golden-image prerequisite is `http: trusted_proxies: [172.30.32.0/23]`
so Tailscale Serve's reverse proxy is accepted.
