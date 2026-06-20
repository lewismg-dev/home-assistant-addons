#!/usr/bin/env python3
"""
Brightgate Connector — add-on-owned remote-support service.

Responsibilities (all inside the add-on; nothing editable in HA):
  1. Enrollment    — one-time code -> portal /api/enroll -> creds in /data.
  2. Heartbeat     — outbound liveness/telemetry to the portal every 10 min
                     (full field set: version, cpu/mem/disk, temp, updates,
                     backup, watchman, entity health, uptimes + tunnel state).
  3. Events        — listens for the HA event "brightgate_support_event" (raised
                     by the notify_brightgate script) and forwards to the portal,
                     so HA automations can alert Brightgate with no secret in HA.
  4. Session       — homeowner grant/revoke via the Ingress panel + auto-revoke.
  5. Tunnel        — bring Tailscale up + publish Serve only during a session.

No `map: config` — this service cannot read or write /config by design.
"""
import asyncio
import json
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path

import aiohttp
from aiohttp import web

# ── Paths & config ────────────────────────────────────────────────────────────
DATA = Path("/data")
CREDS_FILE = DATA / "connector.json"     # enrollment result
SESSION_FILE = DATA / "session.json"     # current grant state
OPTIONS_FILE = DATA / "options.json"     # add-on options
TS_SOCKET = os.environ.get("TS_SOCKET", "/var/run/tailscale/tailscaled.sock")

SUPERVISOR_TOKEN = os.environ.get("SUPERVISOR_TOKEN", "")
CORE_API = "http://supervisor/core/api"
HA_INTERNAL_URL = "http://homeassistant:8123"
HEARTBEAT_INTERVAL_S = 600           # 10 minutes
SESSION_TICK_S = 20                  # how often we check for auto-revoke

# Homeowner-selectable grant durations. None = indefinite (no auto-revoke).
DURATIONS = {
    "1d": 24 * 3600,
    "1w": 7 * 24 * 3600,
    "3mo": 90 * 24 * 3600,
    "indef": None,
}
DEFAULT_DURATION = "1d"

# Tracks an in-progress interactive login (when no auth key is available).
PENDING = {"auth_url": None}


def _opt(key, default=None):
    try:
        return json.loads(OPTIONS_FILE.read_text()).get(key, default)
    except Exception:
        return default


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _read_json(path, default):
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def _write_json(path, obj):
    path.write_text(json.dumps(obj, indent=2))


def creds():
    return _read_json(CREDS_FILE, {})


def session():
    return _read_json(SESSION_FILE, {"active": False})


def portal_url():
    return (creds().get("portal_url") or _opt("portal_url")
            or "https://portal.brightgatesolutions.com.au").rstrip("/")


def log(msg):
    print(f"[brightgate] {msg}", flush=True)


# ── Tailscale helpers ─────────────────────────────────────────────────────────
async def ts(*args):
    """Run a tailscale CLI command; return (rc, stdout, stderr)."""
    proc = await asyncio.create_subprocess_exec(
        "tailscale", f"--socket={TS_SOCKET}", *args,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    out, err = await proc.communicate()
    return proc.returncode, out.decode().strip(), err.decode().strip()


async def ts_status():
    rc, out, _ = await ts("status", "--json")
    if rc != 0:
        return {}
    try:
        return json.loads(out)
    except Exception:
        return {}


async def is_registered():
    """True once the node has an identity (Running or Stopped, not NeedsLogin)."""
    return (await ts_status()).get("BackendState") in ("Running", "Stopped")


async def _bg_up(hostname):
    """Interactive `tailscale up` — blocks until the human approves the AuthURL."""
    await ts("up", f"--hostname={hostname}", "--accept-routes=false",
             "--accept-dns=false", "--ssh=false")
    PENDING["auth_url"] = None


async def start_login(c):
    """Kick off interactive registration; return the Tailscale AuthURL to show."""
    hostname = c.get("support_hostname") or "brightgate-ha-support"
    asyncio.create_task(_bg_up(hostname))
    for _ in range(15):
        url = (await ts_status()).get("AuthURL")
        if url:
            PENDING["auth_url"] = url
            return url
        await asyncio.sleep(1)
    return None


async def tunnel_up(c):
    """Ensure the node is registered + connected, then publish Serve.

    Returns (ok, message, auth_url). auth_url is set only when first-time
    interactive login is required (no enrollment auth key available)."""
    hostname = c.get("support_hostname") or "brightgate-ha-support"
    if not await is_registered():
        authkey = c.get("tailscale_authkey")
        if authkey:
            rc, _, err = await ts(
                "up", f"--hostname={hostname}", "--accept-routes=false",
                "--accept-dns=false", "--ssh=false", f"--authkey={authkey}")
            if rc != 0:
                return False, f"registration failed: {err}", None
        else:
            return False, "needs_login", await start_login(c)
    # Registered: reconnect (keyless) and (re)publish Serve.
    await ts("up", f"--hostname={hostname}", "--accept-routes=false",
             "--accept-dns=false", "--ssh=false")
    await ts("serve", "reset")
    rc, _, err = await ts("serve", "--bg", "--https=443", HA_INTERNAL_URL)
    if rc != 0:
        return False, f"serve failed: {err}", None
    PENDING["auth_url"] = None
    return True, "up", None


async def tunnel_down():
    await ts("serve", "reset")
    await ts("down")


async def tunnel_url():
    st = await ts_status()
    dns = (st.get("Self") or {}).get("DNSName", "").rstrip(".")
    return f"https://{dns}" if dns else None


# ── Session control ───────────────────────────────────────────────────────────
async def grant(duration):
    c = creds()
    if not c.get("client_id"):
        return False, "Not enrolled yet.", None
    if duration not in DURATIONS:
        duration = DEFAULT_DURATION
    ok, msg, auth_url = await tunnel_up(c)
    if not ok:
        return False, msg, auth_url
    sess = {"active": True, "granted_at": _now_iso(), "duration": duration}
    secs = DURATIONS[duration]
    if secs is not None:  # None = indefinite -> no expiry, no auto-revoke
        sess["expires_at"] = (datetime.now(timezone.utc) + timedelta(seconds=secs)).isoformat()
    _write_json(SESSION_FILE, sess)
    asyncio.create_task(send_heartbeat())   # report support_access=on promptly
    return True, "granted", None


async def revoke():
    await tunnel_down()
    s = session()
    s.update({"active": False, "revoked_at": _now_iso()})
    _write_json(SESSION_FILE, s)
    asyncio.create_task(send_heartbeat())
    return True, "revoked"


# ── Portal calls ──────────────────────────────────────────────────────────────
async def enroll(code, http):
    code = (code or "").strip()
    if not code:
        return False, "Enrollment code required."
    url = (_opt("portal_url") or "https://portal.brightgatesolutions.com.au").rstrip("/")
    try:
        async with http.post(f"{url}/api/enroll",
                             json={"code": code},
                             timeout=aiohttp.ClientTimeout(total=30)) as r:
            body = await r.json()
            if r.status != 200:
                return False, body.get("error", f"Enrollment failed ({r.status}).")
    except Exception as e:
        return False, f"Could not reach portal: {e}"
    saved = {
        "client_id": body["client_id"],
        "heartbeat_secret": body["heartbeat_secret"],
        "support_hostname": body.get("support_hostname"),
        "portal_url": body.get("portal_url", url),
        "tailscale_authkey": body.get("tailscale_authkey"),
        "enrolled_at": _now_iso(),
    }
    _write_json(CREDS_FILE, saved)
    return True, "enrolled"


def _num(st, eid):
    s = st.get(eid)
    if not s:
        return None
    v = s.get("state")
    if v in (None, "unknown", "unavailable", ""):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _txt(st, eid):
    s = st.get(eid)
    v = s.get("state") if s else None
    return v if v not in (None, "unknown", "unavailable", "") else None


async def ha_stats(http):
    """Best-effort HA telemetry for the heartbeat. All fields optional — the
    portal accepts whatever subset is present. Mirrors the v1 heartbeat so the
    portal alert engine + dashboards keep working for v2 clients."""
    headers = {"Authorization": f"Bearer {SUPERVISOR_TOKEN}"}
    out = {}
    try:
        async with http.get(f"{CORE_API}/config", headers=headers,
                            timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status == 200:
                out["ha_version"] = (await r.json()).get("version")
    except Exception:
        pass
    try:
        async with http.get(f"{CORE_API}/states", headers=headers,
                            timeout=aiohttp.ClientTimeout(total=20)) as r:
            if r.status != 200:
                return out
            arr = await r.json()
    except Exception:
        return out
    st = {s["entity_id"]: s for s in arr if isinstance(s, dict) and "entity_id" in s}
    out["cpu_pct"] = _num(st, "sensor.system_monitor_processor_use")
    out["cpu_temp_c"] = _num(st, "sensor.system_monitor_processor_temperature")
    out["memory_pct"] = _num(st, "sensor.system_monitor_memory_usage")
    out["memory_used_mb"] = _num(st, "sensor.system_monitor_memory_use")
    out["disk_free_gb"] = (_num(st, "sensor.system_monitor_disk_free_config")
                           or _num(st, "sensor.system_monitor_disk_free"))
    out["disk_used_pct"] = _num(st, "sensor.system_monitor_disk_usage_config")
    out["watchman_missing"] = _num(st, "sensor.watchman_missing_entities")
    out["backup_last_ok"] = _txt(st, "sensor.backup_last_successful_automatic_backup")
    out["host_last_boot"] = _txt(st, "sensor.system_monitor_last_boot")
    out["ha_last_started"] = _txt(st, "sensor.uptime")
    out["ip_local"] = _txt(st, "sensor.system_monitor_ipv4_address_eth0")
    now = datetime.now(timezone.utc)
    for src, dst in (("sensor.system_monitor_last_boot", "host_uptime_seconds"),
                     ("sensor.uptime", "ha_uptime_seconds")):
        t = _txt(st, src)
        if t:
            try:
                out[dst] = int((now - datetime.fromisoformat(t)).total_seconds())
            except Exception:
                pass
    out["updates_pending"] = sum(
        1 for s in arr if s.get("entity_id", "").startswith("update.") and s.get("state") == "on")
    out["unavailable_entities"] = sum(1 for s in arr if s.get("state") == "unavailable")
    out["entity_count"] = len(arr)
    return {k: v for k, v in out.items() if v is not None}


async def send_heartbeat(http=None):
    c = creds()
    if not c.get("client_id") or not c.get("heartbeat_secret"):
        return
    own = http is None
    if own:
        http = aiohttp.ClientSession()
    try:
        s = session()
        stats = await ha_stats(http)
        st = await ts_status()
        self_node = st.get("Self") or {}
        payload = {
            "client_id": c["client_id"],
            "reported_at": _now_iso(),
            "support_access": "on" if s.get("active") else "off",
            "support_tunnel_state": "on" if s.get("active") else "off",
            "support_tunnel_hostname": c.get("support_hostname"),
            "support_tunnel_url": await tunnel_url() if s.get("active") else None,
            "support_tunnel_key_expiry_disabled": not self_node.get("KeyExpiry"),
            "support_tunnel_last_granted_at": s.get("granted_at"),
            "support_tunnel_last_revoked_at": s.get("revoked_at"),
            # Grant duration -> monitoring tier. 1d = momentary support; 1w/3mo/
            # indefinite = monitored (portal keeps full monitoring while granted).
            "support_duration": s.get("duration") if s.get("active") else None,
            "support_tier": (
                ("monitored" if s.get("duration") in ("1w", "3mo", "indef") else "momentary")
                if s.get("active") else None
            ),
            **stats,
        }
        url = f"{portal_url()}/api/heartbeat"
        headers = {"Authorization": f"Bearer {c['heartbeat_secret']}"}
        async with http.post(url, json=payload, headers=headers,
                            timeout=aiohttp.ClientTimeout(total=30)) as r:
            log(f"heartbeat -> {r.status}")
    except Exception as e:
        log(f"heartbeat error: {e}")
    finally:
        if own:
            await http.close()


# ── Background loops ──────────────────────────────────────────────────────────
async def heartbeat_loop(app):
    http = app["http"]
    while True:
        await send_heartbeat(http)
        await asyncio.sleep(HEARTBEAT_INTERVAL_S)


async def session_watchdog(app):
    while True:
        s = session()
        if s.get("active") and s.get("expires_at"):
            try:
                if datetime.now(timezone.utc) >= datetime.fromisoformat(s["expires_at"]):
                    log("session expired -> auto-revoke")
                    await revoke()
            except Exception:
                pass
        await asyncio.sleep(SESSION_TICK_S)


# ── Ingress web UI ────────────────────────────────────────────────────────────
PAGE = """<!doctype html><html><head><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1">
<title>Brightgate Support</title><style>
body{font-family:system-ui,sans-serif;max-width:640px;margin:24px auto;padding:0 16px;color:#1b2733}
h1{font-size:1.4rem}.card{border:1px solid #d8e0e8;border-radius:12px;padding:18px;margin:14px 0}
button{font-size:1rem;padding:10px 16px;border-radius:8px;border:0;cursor:pointer}
.grant{background:#0a7d33;color:#fff}.revoke{background:#b3261e;color:#fff}
.muted{color:#5a6b7b;font-size:.9rem}.on{color:#0a7d33;font-weight:600}.off{color:#5a6b7b}
input,select{font-size:1rem;padding:8px;border-radius:8px;border:1px solid #c3ccd6}
a{color:#0a59c2}
</style></head><body>
<h1>🛟 Brightgate Remote Support</h1>
<div id=app class=card>Loading…</div>
<p class=muted>This connection is managed by Brightgate Solutions. While access is
on, our support team can see the live state of your system to help you. It closes
automatically when the timer ends, or when you switch it off.</p>
<script>
async function api(p,opt){const r=await fetch(p,opt);return r.json()}
function h(s){return (s||'').replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]))}
async function render(){
 const s=await api('status');const a=document.getElementById('app');
 if(!s.enrolled){a.innerHTML=`<h3>Activate this machine</h3>
   <p class=muted>Enter the enrollment code from your Brightgate portal.</p>
   <input id=code placeholder="enrollment code" size=28>
   <button class=grant onclick="doEnroll()">Activate</button><div id=msg class=muted></div>`;return}
 if(s.auth_url){a.innerHTML=`<h3>Authorise this support node</h3>
   <p class=muted>One-time step: open this link, approve, then come back and Grant.</p>
   <p><a href="${h(s.auth_url)}" target=_blank rel=noopener>Open Tailscale authorisation ↗</a></p>`;return}
 if(s.active){a.innerHTML=`<p>Support access is <span class=on>ON</span></p>
   <p class=muted>Time remaining: <b>${h(s.remaining)}</b></p>
   <button class=revoke onclick="doRevoke()">Turn off support access</button>`}
 else{a.innerHTML=`<p>Support access is <span class=off>OFF</span></p>
   <label>Grant for:&nbsp;<select id=dur><option value=1d>1 day</option><option value=1w>1 week</option><option value=3mo>3 months</option><option value=indef>Indefinite</option></select></label><br><br>
   <button class=grant onclick="doGrant()">Grant Brightgate support access</button>`}}
async function doEnroll(){document.getElementById('msg').textContent='Activating…';
 const r=await api('enroll',{method:'POST',headers:{'Content-Type':'application/json'},
  body:JSON.stringify({code:document.getElementById('code').value})});
 if(!r.ok)document.getElementById('msg').textContent=r.error||'Failed';else render()}
async function doGrant(){const r=await api('grant',{method:'POST',headers:{'Content-Type':'application/json'},
 body:JSON.stringify({duration:document.getElementById('dur').value})});
 if(r&&r.auth_url){alert('First-time setup: open the authorisation link, approve, then Grant again.')}render()}
async function doRevoke(){await api('revoke',{method:'POST'});render()}
render();setInterval(render,5000);
</script></body></html>"""


def _remaining(s):
    if not s.get("expires_at"):
        return "no expiry (until revoked)"
    try:
        secs = int((datetime.fromisoformat(s["expires_at"]) -
                    datetime.now(timezone.utc)).total_seconds())
        if secs < 0:
            return "0:00"
        d, h, m = secs // 86400, (secs % 86400) // 3600, (secs % 3600) // 60
        if d:
            return f"{d}d {h}h"
        return f"{h}:{m:02d}"
    except Exception:
        return ""


async def h_index(req):
    return web.Response(text=PAGE, content_type="text/html")


async def h_status(req):
    c, s = creds(), session()
    return web.json_response({
        "enrolled": bool(c.get("client_id")),
        "registered": await is_registered(),
        "active": bool(s.get("active")),
        "remaining": _remaining(s) if s.get("active") else None,
        "auth_url": PENDING.get("auth_url"),
        "hostname": c.get("support_hostname"),
    })


async def h_enroll(req):
    body = await req.json()
    ok, msg = await enroll(body.get("code"), req.app["http"])
    return web.json_response({"ok": ok, "error": None if ok else msg})


async def h_grant(req):
    body = await req.json()
    ok, msg, auth_url = await grant(body.get("duration"))
    return web.json_response({"ok": ok, "error": None if ok else msg, "auth_url": auth_url})


async def h_revoke(req):
    await revoke()
    return web.json_response({"ok": True})


async def on_start(app):
    app["http"] = aiohttp.ClientSession()
    app["tasks"] = [asyncio.create_task(heartbeat_loop(app)),
                    asyncio.create_task(session_watchdog(app))]
    log("connector started")


async def on_cleanup(app):
    for t in app["tasks"]:
        t.cancel()
    await app["http"].close()


def main():
    app = web.Application()
    app.add_routes([
        web.get("/", h_index),
        web.get("/status", h_status),
        web.post("/enroll", h_enroll),
        web.post("/grant", h_grant),
        web.post("/revoke", h_revoke),
    ])
    app.on_startup.append(on_start)
    app.on_cleanup.append(on_cleanup)
    web.run_app(app, host="0.0.0.0", port=8099, print=None)


if __name__ == "__main__":
    main()
