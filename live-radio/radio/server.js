// Live Radio server — no dependencies. Run: node server.js
const http = require("http"), fs = require("fs"), path = require("path"), crypto = require("crypto");
const PORT = process.env.PORT || 3000;
const DB = path.join(__dirname, "tasks.json");
const GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

// ---------- tasks ----------
const day = () => new Intl.DateTimeFormat("en-CA", { timeZone: process.env.TZ_NAME || "Asia/Riyadh" }).format(new Date());
let tasks = [];
try { tasks = JSON.parse(fs.readFileSync(DB, "utf8")); } catch (e) {}
const save = () => { try { fs.writeFileSync(DB, JSON.stringify(tasks)); } catch (e) {} };
const todays = () => { const d = day(); tasks = tasks.filter(t => t.date >= d); return tasks.filter(t => t.date === d); };

// ---------- http ----------
const server = http.createServer((req, res) => {
  if (req.url === "/health") { res.end("ok"); return; }
  fs.readFile(path.join(__dirname, "public", "index.html"), (err, data) => {
    if (err) { res.writeHead(500); res.end("error"); return; }
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-cache" });
    res.end(data);
  });
});

// ---------- minimal websocket (RFC 6455) ----------
const clients = new Set();
let talker = null, talkTimer = null;

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
const bcast = (o, except) => { const f = frame(1, Buffer.from(JSON.stringify(o))); clients.forEach(c => { if (c !== except) sendRaw(c, f); }); };

function endTalk(c) {
  if (talker !== c) return;
  talker = null; clearTimeout(talkTimer);
  bcast({ t: "talk", on: false });
}

function onMessage(c, op, data) {
  if (op === 2) { // audio chunk → relay to everyone else
    if (c === talker) { const f = frame(2, data); clients.forEach(o => { if (o !== c) sendRaw(o, f); }); }
    return;
  }
  let m; try { m = JSON.parse(data.toString()); } catch (e) { return; }
  switch (m.t) {
    case "hello": c.name = String(m.name || "مجهول").slice(0, 20); break;
    case "start":
      if (talker && talker !== c) { sendJSON(c, { t: "busy" }); break; }
      talker = c; sendJSON(c, { t: "grant" });
      bcast({ t: "talk", on: true, name: c.name || "مجهول" }, c);
      clearTimeout(talkTimer); talkTimer = setTimeout(() => { sendJSON(c, { t: "timeout" }); endTalk(c); }, 40000);
      break;
    case "end": endTalk(c); break;
    case "add": {
      const text = String(m.text || "").trim().slice(0, 120), time = /^\d\d:\d\d$/.test(m.time) ? m.time : "00:00";
      if (!text) break;
      tasks.push({ id: crypto.randomBytes(5).toString("hex"), time, text, date: day(), done: false }); save();
      bcast({ t: "tasks", tasks: todays() }); break;
    }
    case "toggle": { const t = tasks.find(x => x.id === m.id); if (t) { t.done = !t.done; save(); bcast({ t: "tasks", tasks: todays() }); } break; }
    case "del": tasks = tasks.filter(x => x.id !== m.id); save(); bcast({ t: "tasks", tasks: todays() }); break;
  }
}

server.on("upgrade", (req, socket) => {
  const key = req.headers["sec-websocket-key"];
  if (!key) { socket.destroy(); return; }
  const accept = crypto.createHash("sha1").update(key + GUID).digest("base64");
  socket.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + accept + "\r\n\r\n");
  socket.setNoDelay(true);
  const c = { socket, name: "مجهول" };
  clients.add(c);
  sendJSON(c, { t: "tasks", tasks: todays() });
  bcast({ t: "peers", n: clients.size });
  let buf = Buffer.alloc(0), partial = null;

  const close = () => {
    if (!clients.delete(c)) return;
    endTalk(c); bcast({ t: "peers", n: clients.size }); socket.destroy();
  };
  socket.on("data", chunk => {
    buf = Buffer.concat([buf, chunk]);
    while (buf.length >= 2) {
      const fin = (buf[0] & 0x80) !== 0, op = buf[0] & 0x0f, masked = (buf[1] & 0x80) !== 0;
      let len = buf[1] & 0x7f, off = 2;
      if (len === 126) { if (buf.length < 4) return; len = buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (buf.length < 10) return; len = Number(buf.readBigUInt64BE(2)); off = 10; }
      if (len > 262144) { close(); return; }
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
      if (op === 0) { partial.data = Buffer.concat([partial.data, payload]); if (fin) { onMessage(c, partial.op, partial.data); partial = null; } continue; }
      if (!fin) { partial = { op, data: payload }; continue; }
      onMessage(c, op, payload);
    }
  });
  socket.on("close", close); socket.on("error", close);
});

setInterval(() => clients.forEach(c => { try { sendRaw(c, frame(9, Buffer.alloc(0))); } catch (e) {} }), 25000);
server.listen(PORT, () => console.log("Live Radio running on port " + PORT));
