# Brightgate Solutions Home Assistant Add-ons

Home Assistant add-on repository for Brightgate Solutions managed installs.

## Add This Repository

In a client's Home Assistant: **Settings -> Add-ons -> Add-on Store -> menu -> Repositories**,
add this repository URL, then install **Brightgate Connector**.

## Add-ons

### Brightgate Connector

Self-enrolling remote-support connector for Brightgate Solutions.

The connector owns:

- one-time enrollment with the Brightgate portal
- outbound heartbeat and system-health telemetry
- homeowner grant/revoke control in its own Ingress panel
- on-demand Tailscale Serve tunnel for Brightgate support

The support tunnel is off unless the homeowner grants access. During a granted
session it exposes **only Home Assistant** to the Brightgate tailnet via Tailscale
Serve on HTTPS 443. It does not advertise subnet routes, does not run as an exit
node, does not enable Tailscale SSH, and uses userspace networking so it can run
alongside a client's own Tailscale add-on.

See [`brightgate_support_tunnel/README.md`](brightgate_support_tunnel/README.md)
for details.

## Per-Machine Install

1. Add this repository and install **Brightgate Connector**.
2. Open the add-on panel.
3. Paste the single-use enrollment code generated in the Brightgate portal.
4. If the portal did not provide a zero-touch Tailscale auth key, approve the
   one-time Tailscale login URL shown in the add-on panel.
5. In the Tailscale admin console, ensure the live support node has
   `tag:ha-support`, node key expiry disabled, and is not ephemeral.
6. For monitored clients, store the Home Assistant long-lived access token in the
   Brightgate portal so bridge polling can populate System Health during monitored
   support grants.

No per-client Home Assistant YAML is required. Do not add Brightgate helpers,
automations, `rest_command`s, dashboard cards, client IDs, heartbeat secrets, or
Tailscale auth keys to `/config`.

## Golden-Image Requirement

The only Home Assistant config expected in the golden image is the trusted proxy
setting needed for Tailscale Serve's reverse proxy:

```yaml
http:
  use_x_forwarded_for: true
  trusted_proxies:
    - 172.30.32.0/23
```

## Security Model

- No Tailscale keys, HA tokens, portal service keys, heartbeat secrets, client IDs,
  or customer-specific hostnames are stored in this repository.
- Enrollment codes are single-use and expire in the portal.
- Per-client credentials are returned only during enrollment and stored in the
  add-on's `/data` directory.
- If the portal supplies a first-registration Tailscale auth key, the connector
  removes it from `/data` after successful registration.
- The add-on does not map `/config`, so it cannot read or edit Home Assistant
  configuration files.
- Access control is enforced by Tailscale ACLs, the `tag:ha-support` device tag,
  the portal heartbeat secret, and homeowner-controlled grant/revoke.

## Support Durations

- `1 day` grants momentary support access.
- `1 week`, `3 months`, and `Indefinite` grants are reported as monitored support
  sessions.

The Brightgate portal decides how those tiers affect monitoring, alerting, and
client-facing System Health.
