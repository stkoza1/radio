// Live Radio server — no dependencies. Run: node server.js
const http = require("http"), fs = require("fs"), path = require("path"), crypto = require("crypto");
const PORT = process.env.PORT || 3000;
const DB = path.join(__dirname, "tasks.json");
const GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const MAX_FRAME = 262144;
const MAX_TASKS = 100;
const SESSION_MS = 12 * 60 * 60 * 1000;
const MAX_SESSIONS = 2000;

// ---------- tasks ----------
const day = () => new Intl.DateTimeFormat("en-CA", { timeZone: process.env.TZ_NAME || "Asia/Riyadh" }).format(new Date());
let tasks = [];
try { tasks = JSON.parse(fs.readFileSync(DB, "utf8")); } catch (e) {}
const save = () => { try { fs.writeFileSync(DB, JSON.stringify(tasks)); } catch (e) {} };
const todays = () => { const d = day(); tasks = tasks.filter(t => t.date >= d); return tasks.filter(t => t.date === d); };

// ---------- sessions ----------
const sessions = new Map();
const parseCookie = (req, name) => {
  const raw = req.headers.cookie || "";
  for (const part of raw.split(";")) {
    const i = part.indexOf("=");
    if (i === -1) continue;
    if (part.slice(0, i).trim() === name) {
      try { return decodeURIComponent(part.slice(i + 1).trim()); } catch (e) { return ""; }
    }
  }
  return "";
};
const pruneSessions = () => {
  const now = Date.now();
  for (const [id, row] of sessions) if (row.exp < now) sessions.delete(id);
};
const issueSession = () => {
  pruneSessions();
  while (sessions.size >= MAX_SESSIONS) sessions.delete(sessions.keys().next().value);
  const id = crypto.randomBytes(24).toString("hex");
  sessions.set(id, { exp: Date.now() + SESSION_MS, proof: crypto.randomBytes(24).toString("hex") });
  return id;
};
const ensureSession = (req) => {
  const existing = parseCookie(req, "sid");
  const row = sessions.get(existing);
  if (row && row.exp > Date.now()) { row.exp = Date.now() + SESSION_MS; return existing; }
  return issueSession();
};
const cookieHeader = (id, req) => {
  const proto = String(req.headers["x-forwarded-proto"] || "").split(",")[0].trim();
  return "sid=" + id + "; HttpOnly; SameSite=Strict; Path=/; Max-Age=43200" + (proto === "https" ? "; Secure" : "");
};
const originOk = (req) => {
  const origin = req.headers.origin;
  if (!origin || typeof origin !== "string") return false;
  let url;
  try { url = new URL(origin); } catch (e) { return false; }
  if (url.protocol !== "http:" && url.protocol !== "https:") return false;
  const hosts = [];
  for (const h of [req.headers.host, req.headers["x-forwarded-host"]]) {
    if (!h) continue;
    for (const part of String(h).split(",")) { const t = part.trim(); if (t) hosts.push(t); }
  }
  return hosts.includes(url.host);
};
const rejectUpgrade = (socket, status) => {
  socket.write("HTTP/1.1 " + status + "\r\nConnection: close\r\nContent-Length: 0\r\n\r\n");
  socket.destroy();
};

// ---------- http ----------
const JSON_HEADERS = { "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store" };
const readBody = (req, limit) => new Promise((resolve, reject) => {
  const chunks = [];
  let size = 0;
  req.on("data", c => { size += c.length; if (size > limit) { reject(new Error("big")); req.destroy(); return; } chunks.push(c); });
  req.on("end", () => resolve(Buffer.concat(chunks)));
  req.on("error", reject);
});
const server = http.createServer((req, res) => {
  const pathOnly = (req.url || "/").split("?")[0];
  if (pathOnly === "/health") { res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8", "Cache-Control": "no-store" }); res.end("ok"); return; }
  if (pathOnly === "/poll") { handlePoll(req, res); return; }
  if (pathOnly === "/in") { handleIn(req, res); return; }
  if (pathOnly !== "/" || req.method !== "GET") { res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" }); res.end("not found"); return; }
  fs.readFile(path.join(__dirname, "public", "index.html"), (err, data) => {
    if (err) { res.writeHead(500); res.end("error"); return; }
    const sid = ensureSession(req);
    const html = data.toString("utf8").replaceAll("{{NONCE}}", sessions.get(sid).proof);
    res.writeHead(200, {
      "Content-Type": "text/html; charset=utf-8",
      "Cache-Control": "no-store",
      "Set-Cookie": cookieHeader(sid, req),
    });
    res.end(html);
  });
});

// ---------- minimal websocket (RFC 6455) ----------
const clients = new Set();
const httpClients = new Map();
let talker = null, talkTimer = null;
const onlineCount = () => {
  const now = Date.now();
  let n = clients.size;
  for (const c of httpClients.values()) if (c.seen > now - 20000) n++;
  return n;
};
function enqueue(c, obj) {
  if (obj.t === "audio" && c.queue.some(e => e.t === "audio")) {
    const audios = c.queue.filter(e => e.t === "audio").length;
    if (audios >= 8) return;
  }
  c.queue.push(obj);
  if (c.queue.length > 40) c.queue.splice(0, c.queue.length - 40);
  flush(c);
}
function flush(c) {
  if (!c.res || !c.queue.length) return;
  const res = c.res;
  c.res = null;
  clearTimeout(c.timer);
  const events = c.queue.splice(0);
  res.writeHead(200, JSON_HEADERS);
  res.end(JSON.stringify({ events, peers: onlineCount() }));
}
function reply(c, obj) { if (c.http) enqueue(c, obj); else sendJSON(c, obj); }

function frame(op, payload) {
  const len = payload.length;
  let head;
  if (len < 126) head = Buffer.from([0x80 | op, len]);
  else if (len < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | op; head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[0] = 0x80 | op; head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2); }
  return Buffer.concat([head, payload]);
}
const sendRaw = (c, buf) => { if (!c.socket.destroyed) c.socket.write(buf); };
const sendJSON = (c, o) => sendRaw(c, frame(1, Buffer.from(JSON.stringify(o))));
const bcast = (o, except) => {
  clients.forEach(c => { if (c !== except) sendJSON(c, o); });
  httpClients.forEach(c => { if (c !== except && c.authed) enqueue(c, o); });
};
function bcastAudio(except, buf) {
  const f = frame(2, buf);
  clients.forEach(c => { if (c !== except) sendRaw(c, f); });
  const b64 = buf.toString("base64");
  httpClients.forEach(c => { if (c !== except && c.authed) enqueue(c, { t: "audio", b64 }); });
}
function bcastPeers() { bcast({ t: "peers", n: onlineCount() }); }

function endTalk(c) {
  if (talker !== c) return;
  talker = null; clearTimeout(talkTimer);
  bcast({ t: "talk", on: false });
}

function onMessage(c, op, data) {
  if (op === 2) {
    if (c.authed && c === talker) bcastAudio(c, data);
    return;
  }
  if (op !== 1) return;
  let m; try { m = JSON.parse(data.toString()); } catch (e) { return; }
  if (!m || typeof m.t !== "string") return;
  if (m.t === "hello") {
    const row = sessions.get(c.sid);
    if (!row || typeof m.proof !== "string" || m.proof.length > 64 || m.proof !== row.proof) { c.drop(); return; }
    c.authed = true;
    c.name = String(m.name || "مجهول").slice(0, 20);
    if (c.http) { enqueue(c, { t: "tasks", tasks: tasksFor(c) }); bcastPeers(); }
    return;
  }
  if (!c.authed) return;
  switch (m.t) {
    case "start":
      if (talker && talker !== c) { reply(c, { t: "busy" }); break; }
      talker = c; reply(c, { t: "grant" });
      bcast({ t: "talk", on: true, name: c.name || "مجهول" }, c);
      clearTimeout(talkTimer); talkTimer = setTimeout(() => { reply(c, { t: "timeout" }); endTalk(c); }, 40000);
      break;
    case "end": endTalk(c); break;
    case "add": {
      const text = String(m.text || "").trim().slice(0, 120), time = /^\d\d:\d\d$/.test(m.time) ? m.time : "00:00";
      if (!text) break;
      if (todays().length >= MAX_TASKS) { reply(c, { t: "full" }); break; }
      tasks.push({ id: crypto.randomBytes(5).toString("hex"), time, text, date: day(), done: false, owner: c.sid }); save();
      bcastTasks(); break;
    }
    case "toggle": {
      const t = tasks.find(x => x.id === m.id);
      if (t && t.owner === c.sid) { t.done = !t.done; save(); bcastTasks(); }
      break;
    }
    case "del": {
      const before = tasks.length;
      tasks = tasks.filter(x => x.id !== m.id || x.owner !== c.sid);
      if (tasks.length !== before) { save(); bcastTasks(); }
      break;
    }
  }
}

function tasksFor(c) {
  return todays().map(t => ({ id: t.id, time: t.time, text: t.text, date: t.date, done: t.done, mine: t.owner === c.sid }));
}
function bcastTasks() {
  clients.forEach(c => sendJSON(c, { t: "tasks", tasks: tasksFor(c) }));
  httpClients.forEach(c => { if (c.authed) enqueue(c, { t: "tasks", tasks: tasksFor(c) }); });
}
function sessionRow(req) {
  const sid = parseCookie(req, "sid");
  const row = sessions.get(sid);
  if (!row || row.exp < Date.now()) return null;
  row.exp = Date.now() + SESSION_MS;
  return { sid, row };
}
function handlePoll(req, res) {
  if (req.method !== "GET" || !originOk(req)) { res.writeHead(403); res.end(); return; }
  const found = sessionRow(req);
  const c = found && httpClients.get(found.sid);
  if (!found || !c || !c.authed) { res.writeHead(401); res.end(); return; }
  c.seen = Date.now();
  if (c.res) { const old = c.res; c.res = null; clearTimeout(c.timer); try { old.writeHead(204); old.end(); } catch (e) {} }
  c.res = res;
  if (c.queue.length) { flush(c); return; }
  c.timer = setTimeout(() => {
    if (c.res !== res) return;
    c.res = null;
    res.writeHead(200, JSON_HEADERS);
    res.end(JSON.stringify({ events: [], peers: onlineCount() }));
  }, 3000);
  req.on("close", () => { if (c.res === res) { c.res = null; clearTimeout(c.timer); } });
}
function handleIn(req, res) {
  if (req.method !== "POST" || !originOk(req)) { res.writeHead(403); res.end(); return; }
  const found = sessionRow(req);
  if (!found) { res.writeHead(401); res.end(); return; }
  readBody(req, 65536).then(body => {
    const type = String(req.headers["content-type"] || "");
    if (type.startsWith("application/octet-stream")) {
      const c = httpClients.get(found.sid);
      if (c && c.authed && talker === c && body.length && body.length <= MAX_FRAME) bcastAudio(c, body);
      res.writeHead(204); res.end();
      return;
    }
    let m; try { m = JSON.parse(body.toString()); } catch (e) { res.writeHead(400); res.end(); return; }
    const existing = httpClients.get(found.sid);
    if (!m || typeof m.t !== "string" || (m.t !== "hello" && !(existing && existing.authed))) { res.writeHead(401); res.end(); return; }
    while (httpClients.size >= 300) {
      const oldest = [...httpClients.values()].sort((a, b) => a.seen - b.seen)[0];
      if (!oldest) break;
      httpClients.delete(oldest.sid);
    }
    const c = httpClients.get(found.sid) || { sid: found.sid, name: "مجهول", authed: false, http: true, queue: [], res: null, timer: null, seen: Date.now(), drop() { httpClients.delete(this.sid); if (talker === this) endTalk(this); } };
    c.seen = Date.now();
    c.http = true;
    httpClients.set(found.sid, c);
    onMessage(c, 1, Buffer.from(JSON.stringify(m)));
    if (!res.headersSent) { res.writeHead(c.authed || m.t !== "hello" ? 204 : 401); res.end(); }
  }).catch(() => { if (!res.headersSent) { res.writeHead(413); res.end(); } });
}

server.on("upgrade", (req, socket) => {
  const key = req.headers["sec-websocket-key"];
  if (!key || !originOk(req)) { rejectUpgrade(socket, "403 Forbidden"); return; }
  const sid = parseCookie(req, "sid");
  const row = sessions.get(sid);
  if (!row || row.exp < Date.now()) { rejectUpgrade(socket, "401 Unauthorized"); return; }
  const accept = crypto.createHash("sha1").update(key + GUID).digest("base64");
  socket.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + accept + "\r\n\r\n");
  socket.setNoDelay(true);
  const c = { socket, name: "مجهول", sid, authed: false, drop: () => {} };
  clients.add(c);
  sendJSON(c, { t: "tasks", tasks: tasksFor(c) });
  bcastPeers();
  let buf = Buffer.alloc(0), partial = null;

  const close = () => {
    if (!clients.delete(c)) return;
    endTalk(c); bcastPeers(); socket.destroy();
  };
  c.drop = close;
  socket.on("data", chunk => {
    try {
      buf = Buffer.concat([buf, chunk]);
      if (buf.length > MAX_FRAME + 14) { close(); return; }
      while (buf.length >= 2) {
        const fin = (buf[0] & 0x80) !== 0, op = buf[0] & 0x0f, masked = (buf[1] & 0x80) !== 0;
        let len = buf[1] & 0x7f, off = 2;
        if (len === 126) { if (buf.length < 4) return; len = buf.readUInt16BE(2); off = 4; }
        else if (len === 127) { if (buf.length < 10) return; len = Number(buf.readBigUInt64BE(2)); off = 10; }
        if (!Number.isFinite(len) || len < 0 || len > MAX_FRAME) { close(); return; }
        if (buf.length < off + (masked ? 4 : 0) + len) return;
        let payload;
        if (masked) {
          const mask = buf.slice(off, off + 4); payload = Buffer.from(buf.slice(off + 4, off + 4 + len));
          for (let i = 0; i < len; i++) payload[i] ^= mask[i & 3];
          off += 4;
        } else payload = buf.slice(off, off + len);
        buf = buf.slice(off + len);
        if (op === 8) { close(); return; }
        if (op === 9) { sendRaw(c, frame(10, payload)); continue; }
        if (op === 10) continue;
        if (op === 0) {
          if (!partial || partial.data.length + payload.length > MAX_FRAME) { close(); return; }
          partial.data = Buffer.concat([partial.data, payload]);
          if (fin) { onMessage(c, partial.op, partial.data); partial = null; }
          continue;
        }
        if (op !== 1 && op !== 2) continue;
        if (!fin) {
          if (partial || payload.length > MAX_FRAME) { close(); return; }
          partial = { op, data: payload };
          continue;
        }
        onMessage(c, op, payload);
      }
    } catch (e) { close(); }
  });
  socket.on("close", close); socket.on("error", close);
});

setInterval(() => {
  clients.forEach(c => { try { sendRaw(c, frame(9, Buffer.alloc(0))); } catch (e) {} });
  const now = Date.now();
  for (const [sid, c] of httpClients) if (c.seen < now - 60000) { if (talker === c) endTalk(c); httpClients.delete(sid); }
}, 25000);
server.listen(PORT, () => console.log("Live Radio running on port " + PORT));
