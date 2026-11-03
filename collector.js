// Keeps an AISstream connection open and remembers ships around you.
// Run:  npm i ws   then   AISSTREAM_KEY=xxx TOKEN=yyy FIREBASE_DB=https://... ROOM=... node collector.js   (Node 18+)
// FIREBASE_DB + ROOM (same values as config.js) let it remember every ship's size/type forever.
const WebSocket = require("ws"), http = require("http");
const env = k => (process.env[k] || "").trim(), KEY = env("AISSTREAM_KEY"), TOKEN = env("TOKEN"), PORT = process.env.PORT || 8080;
const DB = env("FIREBASE_DB").replace(/\/+$/, ""), ROOM = env("ROOM"), SURL = DB && ROOM && `${DB}/static/${ROOM}.json`;
if (!KEY) { console.log("Set AISSTREAM_KEY"); process.exit(1); }
const V = new Map(), S = new Map(), dirty = new Set(); let ws, center = null, lastErr = "", msgs = 0, wait = 0, timer;   // V = live ships, S = permanent memory

const typeName = c => c >= 80 && c < 90 ? "Tanker" : c >= 70 && c < 80 ? "Cargo ship" : c >= 60 && c < 70 ? "Passenger ship" :
  ({ 30: "Fishing vessel", 31: "Tug", 32: "Tug", 33: "Dredger", 34: "Diving vessel", 35: "Naval vessel", 36: "Sailing vessel",
     37: "Pleasure craft", 50: "Pilot vessel", 51: "SAR vessel", 52: "Tug", 53: "Port tender", 55: "Law enforcement" }[c] || (c ? "Vessel" : ""));
// Flag = country code in the first 3 digits of the ship’s MMSI number
const FLAGS = Object.fromEntries("201 Albania,202 Andorra,203 Austria,204 Portugal,205 Belgium,206 Belarus,207 Bulgaria,208 Vatican,209 Cyprus,210 Cyprus,211 Germany,212 Cyprus,213 Georgia,214 Moldova,215 Malta,216 Armenia,218 Germany,219 Denmark,220 Denmark,224 Spain,225 Spain,226 France,227 France,228 France,229 Malta,230 Finland,231 Faroe Islands,232 United Kingdom,233 United Kingdom,234 United Kingdom,235 United Kingdom,236 Gibraltar,237 Greece,238 Croatia,239 Greece,240 Greece,241 Greece,242 Morocco,243 Hungary,244 Netherlands,245 Netherlands,246 Netherlands,247 Italy,248 Malta,249 Malta,250 Ireland,251 Iceland,252 Liechtenstein,253 Luxembourg,254 Monaco,255 Portugal,256 Malta,257 Norway,258 Norway,259 Norway,261 Poland,262 Montenegro,263 Portugal,264 Romania,265 Sweden,266 Sweden,267 Slovakia,268 San Marino,269 Switzerland,270 Czechia,271 Türkiye,272 Ukraine,273 Russia,274 North Macedonia,275 Latvia,276 Estonia,277 Lithuania,278 Slovenia,279 Serbia,301 Anguilla,303 United States,304 Antigua and Barbuda,305 Antigua and Barbuda,306 Curaçao,307 Aruba,308 Bahamas,309 Bahamas,310 Bermuda,311 Bahamas,312 Belize,314 Barbados,316 Canada,319 Cayman Islands,321 Costa Rica,323 Cuba,325 Dominica,327 Dominican Republic,329 Guadeloupe,330 Grenada,331 Greenland,332 Guatemala,334 Honduras,336 Haiti,338 United States,339 Jamaica,341 Saint Kitts and Nevis,343 Saint Lucia,345 Mexico,347 Martinique,348 Montserrat,350 Nicaragua,351 Panama,352 Panama,353 Panama,354 Panama,355 Panama,356 Panama,357 Panama,358 Puerto Rico,359 El Salvador,361 Saint Pierre and Miquelon,362 Trinidad and Tobago,364 Turks and Caicos,366 United States,367 United States,368 United States,369 United States,370 Panama,371 Panama,372 Panama,373 Panama,374 Panama,375 Saint Vincent,376 Saint Vincent,377 Saint Vincent,378 British Virgin Islands,379 US Virgin Islands,401 Afghanistan,403 Saudi Arabia,405 Bangladesh,408 Bahrain,410 Bhutan,412 China,413 China,414 China,416 Taiwan,417 Sri Lanka,419 India,422 Iran,423 Azerbaijan,425 Iraq,428 Israel,431 Japan,432 Japan,434 Turkmenistan,436 Kazakhstan,437 Uzbekistan,438 Jordan,440 South Korea,441 South Korea,443 Palestine,445 North Korea,447 Kuwait,450 Lebanon,451 Kyrgyzstan,453 Macao,455 Maldives,457 Mongolia,459 Nepal,461 Oman,463 Pakistan,466 Qatar,468 Syria,470 UAE,471 UAE,472 Tajikistan,473 Yemen,475 Yemen,477 Hong Kong,478 Bosnia and Herzegovina,503 Australia,506 Myanmar,508 Brunei,510 Micronesia,511 Palau,512 New Zealand,514 Cambodia,515 Cambodia,518 Cook Islands,520 Fiji,525 Indonesia,529 Kiribati,531 Laos,533 Malaysia,536 Northern Mariana Islands,538 Marshall Islands,540 New Caledonia,544 Nauru,546 French Polynesia,548 Philippines,553 Papua New Guinea,557 Solomon Islands,559 American Samoa,561 Samoa,563 Singapore,564 Singapore,565 Singapore,566 Singapore,567 Thailand,570 Tonga,572 Tuvalu,574 Vietnam,576 Vanuatu,577 Vanuatu,601 South Africa,603 Angola,605 Algeria,609 Burundi,610 Benin,611 Botswana,612 Central African Republic,613 Cameroon,615 Congo,616 Comoros,617 Cabo Verde,619 Côte d’Ivoire,620 Comoros,621 Djibouti,622 Egypt,624 Ethiopia,625 Eritrea,626 Gabon,627 Ghana,629 Gambia,630 Guinea-Bissau,631 Equatorial Guinea,632 Guinea,633 Burkina Faso,634 Kenya,636 Liberia,637 Liberia,638 South Sudan,642 Libya,644 Lesotho,645 Mauritius,647 Madagascar,649 Mali,650 Mozambique,654 Mauritania,655 Malawi,656 Niger,657 Nigeria,659 Namibia,660 Réunion,661 Rwanda,662 Sudan,663 Senegal,664 Seychelles,665 Saint Helena,666 Somalia,667 Sierra Leone,668 São Tomé and Príncipe,669 Eswatini,670 Chad,671 Togo,672 Tunisia,674 Tanzania,675 Uganda,676 DR Congo,677 Tanzania,678 Zambia,679 Zimbabwe,701 Argentina,710 Brazil,720 Bolivia,725 Chile,730 Colombia,735 Ecuador,740 Falkland Islands,745 French Guiana,750 Guyana,755 Paraguay,760 Peru,765 Suriname,770 Uruguay,775 Venezuela".split(",").map(x => [x.slice(0, 3), x.slice(4)]));
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
  if (m.error) { lastErr = String(m.error); console.log("aisstream says:", lastErr); return; }
  msgs++; wait = 0;   // real data arrived: reset the retry delay
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
    if (t) k.type = t; k.name = v.name || k.name; if (s && s.ImoNumber) k.imo = s.ImoNumber;
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
  return [...out.values()].filter(v => v.name).map(v => ({ ...v, flag: FLAGS[String(v.mmsi).slice(0, 3)] || "", ready: !!v.length }));
};

// One connection at a time. After a refusal (e.g. HTTP 429) wait longer each time: 30 s, 60 s ... up to 10 min
function listen(lat, lon, r, delay = 3000) {
  center = [lat, lon]; clearTimeout(timer);
  timer = setTimeout(() => connect(lat, lon, r), delay);
}
function connect(lat, lon, r) {
  if (ws) { const old = ws; ws = null; old.removeAllListeners(); old.on("error", () => {}); old.terminate(); }
  const dl = r / 60, dn = r / (60 * Math.cos(lat * Math.PI / 180)), me = (ws = new WebSocket("wss://stream.aisstream.io/v0/stream"));
  me.on("open", () => me.send(JSON.stringify({ APIKey: KEY, BoundingBoxes: [[[lat - dl, lon - dn], [lat + dl, lon + dn]]],
    FilterMessageTypes: ["PositionReport", "StandardClassBPositionReport", "ShipStaticData", "StaticDataReport"] })));
  me.on("message", onMsg);
  me.on("error", e => { lastErr = e.message; console.log("ws error:", e.message); });
  me.on("close", () => {
    if (me !== ws) return;
    wait = Math.min(wait ? wait * 2 : 30000, 600000); console.log("reconnecting in", wait / 1000, "s");
    timer = setTimeout(() => connect(lat, lon, r), wait);
  });
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
  res.end(JSON.stringify({ ok: true, live: V.size, known: S.size, messages: msgs, listening: !!center, error: lastErr, retrySeconds: wait / 1000 }));
}).listen(PORT, () => console.log("Collector on port", PORT));
