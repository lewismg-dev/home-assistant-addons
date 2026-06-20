# Brightgate Support Tunnel

On-demand [Tailscale](https://tailscale.com) Serve node for Brightgate Solutions remote
support. It is **off by default** and, when started, exposes **only Home Assistant**
to the Brightgate tailnet (`support@brightgatesolutions.com.au`). No subnet routes, no
exit node, no DNS — minimal blast radius. It runs in **userspace networking** mode so it
never conflicts with a client's own (kernel-mode) Tailscale add-on.

## Options
| Option | Meaning |
|--------|---------|
| `authkey` | Ephemeral, tagged Tailscale auth key (`tag:ha-support`). Leave blank to authenticate interactively via the log. Never stored in YAML/dashboards. |
| `hostname` | Node name on the tailnet, e.g. `client-smith-ha-support`. |
| `ha_url` | Local Home Assistant URL to proxy. Default `http://homeassistant:8123`. |

## How access works
1. Brightgate (or the portal) sets a fresh ephemeral `authkey` and **starts** the add-on.
2. The node joins the tailnet and serves HA at `https://<hostname>.<tailnet>.ts.net`.
3. **Stopping** the add-on removes the Serve proxy; the ephemeral node drops off the tailnet.

## Requirements on the Brightgate tailnet
- MagicDNS + HTTPS certificates **enabled** (needed for `tailscale serve --https`).
- An ACL granting Brightgate admins access to `tag:ha-support:443` only.
- Ephemeral, reusable, tagged auth keys (or an OAuth client) for provisioning.

## Replication
This add-on lives in the Brightgate add-on repository. New client installs add the repo
URL once (or via the portal) and install — no per-client file copying.
