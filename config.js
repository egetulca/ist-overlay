// Public, non-secret defaults only. Your private settings (database address, room, token)
// are NOT stored in this file: they live in the controller's Settings and in the overlay link.
(function () {
  const dec = s => JSON.parse(atob(s.replace(/-/g, "+").replace(/_/g, "/")));
  const OK = ["DB", "ROOM", "COLLECTOR", "TOKEN", "HOME"];
  let saved = {};
  try { saved = JSON.parse(localStorage.ist || "{}"); } catch (e) {}
  const m = location.hash.match(/setup=([\w-]+)/);               // phone setup link
  if (m) {
    try {
      const d = dec(m[1]); saved = {}; OK.forEach(k => d[k] && (saved[k] = d[k]));
      localStorage.ist = JSON.stringify(saved); history.replaceState(null, "", location.pathname + location.search);
    } catch (e) {}
  }
  const q = new URLSearchParams(location.search), link = {};     // overlay link: ?db=...&room=...
  if (q.get("db")) link.DB = q.get("db").replace(/\/+$/, "");
  if (q.get("room")) link.ROOM = q.get("room");
  window.CFG = Object.assign({ HOLD: 20, RADIUS_NM: 25 }, saved, link);   // HOLD = seconds a card stays on screen
})();
