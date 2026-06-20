#!/usr/bin/env python3
"""
Brightgate Connector — add-on-owned remote-support service.

Responsibilities (all inside the add-on; nothing editable in HA):
  1. Enrollment    — one-time code -> portal /api/enroll -> creds in /data.
  2. Heartbeat     — outbound liveness/telemetry to the portal every 10 min.
  3. Session       — homeowner grant/revoke via the Ingress panel, with an
                     auto-revoke timer.
  4. Tunnel        — bring Tailscale up + publish Serve only during a session.

No `map: config` — this service cannot read or write /config by design.
"""
import asyncio
import json
import os
import time
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
DEFAULT_DURATION_H = 1

# System Monitor sensors we opportunistically include in the heartbeat. Missing
# ones are simply omitted — the portal treats them all as optional.
STAT_SENSORS = {
    "cpu_pct": "sensor.system_monitor_processor_use",
    "memory_pct": "sensor.system_monitor_memory_usage",
    "disk_free_gb": "sensor.system_monitor_disk_free",
}


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


async def tunnel_up(c):
    """Bring the node up (authkey only on first registration) and publish Serve."""
    hostname = c.get("support_hostname") or "brightgate-ha-support"
    up = ["up", f"--hostname={hostname}", "--accept-routes=false",
          "--accept-dns=false", "--ssh=false"]
    st = await ts_status()
    needs_login = st.get("BackendState") in (None, "NeedsLogin", "NoState")
    authkey = c.get("tailscale_authkey")
    if needs_login and authkey:
        up.append(f"--authkey={authkey}")
    rc, _, err = await ts(*up)
    if rc != 0:
        return False, f"tailscale up failed: {err}"
    await ts("serve", "reset")
    rc, _, err = await ts("serve", "--bg", "--https=443", HA_INTERNAL_URL)
    if rc != 0:
        return False, f"tailscale serve failed: {err}"
    return True, "up"


async def tunnel_down():
    await ts("serve", "reset")
    await ts("down")


async def tunnel_url():
    st = await ts_status()
    dns = (st.get("Self") or {}).get("DNSName", "").rstrip(".")
    return f"https://{dns}" if dns else None


# ── Session control ───────────────────────────────────────────────────────────
async def grant(hours):
    c = creds()
    if not c.get("client_id"):
        return False, "Not enrolled yet."
    hours = max(1, min(24, int(hours or DEFAULT_DURATION_H)))
    ok, msg = await tunnel_up(c)
    if not ok:
        return False, msg
    expires = datetime.now(timezone.utc) + timedelta(hours=hours)
    _write_json(SESSION_FILE, {
        "active": True, "granted_at": _now_iso(),
        "expires_at": expires.isoformat(), "duration_hours": hours,
    })
    asyncio.create_task(send_heartbeat())   # report support_access=on promptly
    return True, "granted"


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
    # Persist the credentials the portal issued.
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


async def ha_stats(http):
    """Best-effort HA telemetry for the heartbeat (all fields optional)."""
    headers = {"Authorization": f"Bearer {SUPERVISOR_TOKEN}"}
    out = {}
    try:
        async with http.get(f"{CORE_API}/config", headers=headers,
                            timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status == 200:
                out["ha_version"] = (await r.json()).get("version")
    except Exception:
        pass
    for key, entity in STAT_SENSORS.items():
        try:
            async with http.get(f"{CORE_API}/states/{entity}", headers=headers,
                                timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status == 200:
                    val = (await r.json()).get("state")
                    if val not in (None, "unknown", "unavailable"):
                        out[key] = float(val)
        except Exception:
            pass
    return out


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
def log(msg):
    print(f"[brightgate] {msg}", flush=True)


PAGE = """<!doctype html><html><head><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1">
<title>Brightgate Support</title><style>
body{font-family:system-ui,sans-serif;max-width:640px;margin:24px auto;padding:0 16px;color:#1b2733}
h1{font-size:1.4rem}.card{border:1px solid #d8e0e8;border-radius:12px;padding:18px;margin:14px 0}
button{font-size:1rem;padding:10px 16px;border-radius:8px;border:0;cursor:pointer}
.grant{background:#0a7d33;color:#fff}.revoke{background:#b3261e;color:#fff}
.muted{color:#5a6b7b;font-size:.9rem}.on{color:#0a7d33;font-weight:600}.off{color:#5a6b7b}
input,select{font-size:1rem;padding:8px;border-radius:8px;border:1px solid #c3ccd6}
</style></head><body>
<h1>🛟 Brightgate Remote Support</h1>
<div id=app class=card>Loading…</div>
<p class=muted>This connection is managed by Brightgate Solutions. While access is
on, our support team can see the live state of your system to help you. It closes
automatically when the timer ends, or when you switch it off.</p>
<script>
async function api(p,opt){const r=await fetch(p,opt);return r.json()}
function h(s){return s.replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]))}
async function render(){
 const s=await api('status');const a=document.getElementById('app');
 if(!s.enrolled){a.innerHTML=`<h3>Activate this machine</h3>
   <p class=muted>Enter the enrollment code from your Brightgate portal.</p>
   <input id=code placeholder="enrollment code" size=28>
   <button class=grant onclick="doEnroll()">Activate</button><div id=msg class=muted></div>`;return}
 if(s.active){a.innerHTML=`<p>Support access is <span class=on>ON</span></p>
   <p class=muted>Time remaining: <b>${h(s.remaining||'')}</b></p>
   <button class=revoke onclick="doRevoke()">Turn off support access</button>`}
 else{a.innerHTML=`<p>Support access is <span class=off>OFF</span></p>
   <label>Duration:&nbsp;<select id=hours><option>1</option><option>2</option><option>4</option><option>8</option></select> hour(s)</label><br><br>
   <button class=grant onclick="doGrant()">Grant Brightgate support access</button>`}}
async function doEnroll(){document.getElementById('msg').textContent='Activating…';
 const r=await api('enroll',{method:'POST',headers:{'Content-Type':'application/json'},
  body:JSON.stringify({code:document.getElementById('code').value})});
 if(!r.ok)document.getElementById('msg').textContent=r.error||'Failed';else render()}
async function doGrant(){await api('grant',{method:'POST',headers:{'Content-Type':'application/json'},
 body:JSON.stringify({hours:+document.getElementById('hours').value})});render()}
async function doRevoke(){await api('revoke',{method:'POST'});render()}
render();setInterval(render,5000);
</script></body></html>"""


def _remaining(s):
    try:
        secs = int((datetime.fromisoformat(s["expires_at"]) -
                    datetime.now(timezone.utc)).total_seconds())
        if secs < 0:
            return "0:00"
        return f"{secs // 3600}:{(secs % 3600) // 60:02d}"
    except Exception:
        return ""


async def h_index(req):
    return web.Response(text=PAGE, content_type="text/html")


async def h_status(req):
    c, s = creds(), session()
    return web.json_response({
        "enrolled": bool(c.get("client_id")),
        "active": bool(s.get("active")),
        "remaining": _remaining(s) if s.get("active") else None,
        "hostname": c.get("support_hostname"),
    })


async def h_enroll(req):
    body = await req.json()
    ok, msg = await enroll(body.get("code"), req.app["http"])
    return web.json_response({"ok": ok, "error": None if ok else msg})


async def h_grant(req):
    body = await req.json()
    ok, msg = await grant(body.get("hours"))
    return web.json_response({"ok": ok, "error": None if ok else msg})


async def h_revoke(req):
    ok, msg = await revoke()
    return web.json_response({"ok": ok})


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
