// Keeps an AISstream connection open and remembers ships around you.
// Run:  npm i ws   then   AISSTREAM_KEY=xxx TOKEN=yyy FIREBASE_DB=https://... ROOM=... node collector.js   (Node 18+)
// FIREBASE_DB + ROOM (same values as config.js) let it remember every ship's size/type forever.
const WebSocket = require("ws"), http = require("http");
const KEY = process.env.AISSTREAM_KEY, TOKEN = process.env.TOKEN || "", PORT = process.env.PORT || 8080;
const DB = process.env.FIREBASE_DB, ROOM = process.env.ROOM, SURL = DB && ROOM && `${DB}/static/${ROOM}.json`;
if (!KEY) { console.log("Set AISSTREAM_KEY"); process.exit(1); }
const V = new Map(), S = new Map(), dirty = new Set(); let ws, center = null;   // V = live ships, S = permanent memory

const typeName = c => c >= 80 && c < 90 ? "Tanker" : c >= 70 && c < 80 ? "Cargo ship" : c >= 60 && c < 70 ? "Passenger ship" :
  ({ 30: "Fishing vessel", 31: "Tug", 32: "Tug", 33: "Dredger", 34: "Diving vessel", 35: "Naval vessel", 36: "Sailing vessel",
     37: "Pleasure craft", 50: "Pilot vessel", 51: "SAR vessel", 52: "Tug", 53: "Port tender", 55: "Law enforcement" }[c] || (c ? "Vessel" : ""));
const nm = (a, b, c, d) => Math.hypot((c - a) * 60, (d - b) * 60 * Math.cos(a * Math.PI / 180));

// Load the memory at startup, save new knowledge every minute
if (SURL) fetch(SURL).then(r => r.json()).then(o => Object.entries(o || {}).forEach(([k, v]) => S.set(+k, v))).catch(() => {});
setInterval(() => {
  if (!SURL || !dirty.size) return;
  const o = {}; dirty.forEach(id => o[id] = S.get(id)); dirty.clear();
  fetch(SURL, { method: "PATCH", body: JSON.stringify(o) }).catch(() => {});
}, 60000);

function onMsg(buf) {
  let m; try { m = JSON.parse(buf); } catch { return; }
  const md = m.MetaData || {}, id = md.MMSI, mm = m.Message; if (!id || !mm) return;
  const v = V.get(id) || { mmsi: id }; V.set(id, v);
  v.seen = Date.now(); v.lat = md.latitude; v.lon = md.longitude;
  if (md.ShipName && md.ShipName.trim()) {
    v.name = md.ShipName.trim();
    const k = S.get(id); if (k && k.name !== v.name) { k.name = v.name; dirty.add(id); }
  }
  const p = mm.PositionReport || mm.StandardClassBPositionReport, s = mm.ShipStaticData, b = mm.StaticDataReport && mm.StaticDataReport.ReportB, st = s || b;
  if (p) { if (p.Sog < 102.3) v.speed = p.Sog; if (p.Cog < 360) v.course = Math.round(p.Cog); }
  if (st) {
    const d = st.Dimension || {}, k = S.get(id) || { mmsi: id }, t = typeName(s ? s.Type : b.ShipType);
    if (d.A + d.B > 0) { k.length = d.A + d.B; k.breadth = d.C + d.D; }
    if (t) k.type = t; k.name = v.name || k.name;
    S.set(id, k); dirty.add(id);
  }
  if (s) {
    v.to = (s.Destination || "").replace(/@/g, "").trim();
    const e = s.Eta; if (e && e.Day) v.eta = `${e.Day}/${e.Month} ${e.Hour}:${String(e.Minute).padStart(2, "0")}`;
  }
}

// Live data layered on top of memory. "ready" = we know the ship's size
const all = () => {
  const out = new Map(); S.forEach((s, k) => out.set(k, { mmsi: k, ...s })); V.forEach((v, k) => out.set(k, { ...(S.get(k) || {}), ...v }));
  return [...out.values()].filter(v => v.name).map(v => ({ ...v, ready: !!v.length }));
};

function listen(lat, lon, r) {
  center = [lat, lon]; if (ws) ws.close();   // AISstream allows one connection per key
  const dl = r / 60, dn = r / (60 * Math.cos(lat * Math.PI / 180)), me = (ws = new WebSocket("wss://stream.aisstream.io/v0/stream"));
  me.on("open", () => me.send(JSON.stringify({ APIKey: KEY, BoundingBoxes: [[[lat - dl, lon - dn], [lat + dl, lon + dn]]],
    FilterMessageTypes: ["PositionReport", "StandardClassBPositionReport", "ShipStaticData", "StaticDataReport"] })));
  me.on("message", onMsg);
  me.on("error", e => console.log("ws error:", e.message));
  me.on("close", () => { if (me === ws) setTimeout(() => listen(lat, lon, r), 5000); });
}
setInterval(() => V.forEach((v, k) => Date.now() - v.seen > 30 * 60000 && V.delete(k)), 60000);

// After a restart or wake-up, resume listening around your last position (the controller saves it in Firebase)
if (DB && ROOM) fetch(`${DB}/overlay/${ROOM}/loc.json`).then(r => r.json()).then(l => l && l.lat != null && listen(l.lat, l.lon, +process.env.RADIUS_NM || 25)).catch(() => {});

http.createServer((req, res) => {
  const u = new URL(req.url, "http://x"), q = u.searchParams;
  res.setHeader("Access-Control-Allow-Origin", "*"); res.setHeader("Content-Type", "application/json");
  if (TOKEN && q.get("t") !== TOKEN) { res.statusCode = 401; return res.end("{}"); }
  if (u.pathname === "/nearby") {
    const lat = +q.get("lat"), lon = +q.get("lon"), r = +q.get("nm") || 25;
    if (isNaN(lat) || isNaN(lon)) return res.end("{}");
    if (!center || nm(lat, lon, ...center) > r / 2) listen(lat, lon, r);
    const list = all().filter(v => v.lat != null).map(v => ({ ...v, dist: nm(lat, lon, v.lat, v.lon) })).sort((a, b) => a.dist - b.dist).slice(0, 10);
    return res.end(JSON.stringify({ total: V.size, known: S.size, list }));
  }
  if (u.pathname === "/search") {
    const s = (q.get("q") || "").toLowerCase().trim();
    const list = all().filter(v => v.name.toLowerCase().includes(s)).sort((a, b) => (b.seen || 0) - (a.seen || 0)).slice(0, 8);
    return res.end(JSON.stringify({ total: V.size + S.size, list }));
  }
  res.end(JSON.stringify({ ok: true, live: V.size, known: S.size }));
}).listen(PORT, () => console.log("Collector on port", PORT));
