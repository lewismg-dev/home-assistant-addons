# Brightgate Solutions Home Assistant Add-ons

Home Assistant add-on repository for Brightgate Solutions managed installs.

## Add this repository

In a client's Home Assistant: **Settings → Add-ons → Add-on Store → ⋮ → Repositories**,
add this repo's URL, then install **Brightgate Support Tunnel**.

## Add-ons

### Brightgate Support Tunnel
On-demand [Tailscale](https://tailscale.com) Serve node for remote support. Off by
default; when started it exposes **only** Home Assistant to the Brightgate tailnet via
userspace networking (no subnet routes, no exit node, no DNS, no `tailscale0`). It runs
alongside a client's own (kernel-mode) Tailscale add-on without conflict.

See [`brightgate_support_tunnel/README.md`](brightgate_support_tunnel/README.md) for options.

## Per-machine rollout checklist

1. Add this repository and install **Brightgate Support Tunnel**.
2. Set a **unique** `hostname` per site, e.g. `client-<site>-ha-support`.
3. **One-time registration** of the node (first join only):
   - paste a short-lived tagged auth key into the add-on options and start it once, OR
   - start with a blank key and approve the login URL shown in the add-on log.
   After it has joined once, **clear the key** — the node reconnects keyless from its
   saved identity on every future start.
4. In the Tailscale admin console for the new node: **disable key expiry** and make sure
   it is **NOT ephemeral** (the add-on is off most of the time, and ephemeral nodes are
   auto-removed while offline).
5. Apply the Home Assistant homeowner controls from
   [`docs/homeassistant.yaml`](docs/homeassistant.yaml) (helpers + automations + dashboard
   card). **Update the add-on slug** in those automations — a repo-installed add-on is
   `<repo>_brightgate_support_tunnel`, not `local_brightgate_support_tunnel`. Find the
   exact slug in the add-on page URL after install.

## Security notes
- No Tailscale keys or portal secrets are stored in this repository or in client images.
- Access control is enforced by Tailscale ACLs (`tag:ha-support` → Home Assistant `:443`)
  plus the homeowner grant/revoke toggle.
