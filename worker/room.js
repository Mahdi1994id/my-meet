// Zumi SFU — Room Durable Object
// هر اتاق یک DO؛ وبرسوکت Hibernation برای سیگنالینگ سبک و پایدار
// شرکت‌کننده‌ها در storage ذخیره می‌شوند تا با خواب/بیدار شدن DO از دست نروند
// v2: broadcast روستر هنگام تغییر وضعیت (رفع باگ دیده‌نشدن شیراسکرین/میوت) + رله چت و فایل

const MAX_PARTICIPANTS = 12;
const STALE_MS = 70_000; // نبود heartbeat بیش از این = حذف
const ALARM_MS = 30_000;

const MAX_CHAT_LEN = 1200;
const MAX_CHUNK_LEN = 300_000; // ~220KB باینری
const MAX_FILE_NAME = 160;

export class Room {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.participants = null; // { pid: {...} }
  }

  async load() {
    if (this.participants === null) {
      this.participants = (await this.state.storage.get("participants")) || {};
    }
  }

  async save() {
    await this.state.storage.put("participants", this.participants);
    await this.state.storage.setAlarm(Date.now() + ALARM_MS);
  }

  broadcast(msg, exceptWs = null) {
    const data = JSON.stringify(msg);
    for (const ws of this.state.getWebSockets()) {
      if (exceptWs && ws === exceptWs) continue;
      try {
        ws.send(data);
      } catch {
        /* سوکت مرده — webSocketClose خودش پاکسازی می‌کند */
      }
    }
  }

  rosterMsg() {
    return {
      type: "roster",
      participants: Object.values(this.participants).sort((a, b) => a.joinedAt - b.joinedAt),
    };
  }

  pidOf(ws) {
    try {
      return ws.deserializeAttachment()?.pid || null;
    } catch {
      return null;
    }
  }

  async purge() {
    await this.load();
    const now = Date.now();
    let changed = false;
    for (const pid of Object.keys(this.participants)) {
      if (now - this.participants[pid].lastSeen > STALE_MS) {
        delete this.participants[pid];
        changed = true;
      }
    }
    if (changed) {
      await this.save();
      this.broadcast(this.rosterMsg());
    }
  }

  async fetch(request) {
    const pair = new WebSocketPair();
    this.state.acceptWebSocket(pair[1]);
    return new Response(null, { status: 101, webSocket: pair[0] });
  }

  async webSocketMessage(ws, message) {
    await this.load();
    let m;
    try {
      m = JSON.parse(message);
    } catch {
      return;
    }
    if (!m || typeof m.type !== "string") return;

    const senderPid = this.pidOf(ws);

    if (m.type === "join") {
      const pid = String(m.pid || "").slice(0, 64);
      if (!pid) return;
      const exists = !!this.participants[pid];
      if (!exists && Object.keys(this.participants).length >= MAX_PARTICIPANTS) {
        ws.send(JSON.stringify({ type: "error", code: "room_full", message: "ظرفیت اتاق پر است (حداکثر ۱۲ نفر)" }));
        return;
      }
      this.participants[pid] = {
        pid,
        name: String(m.name || "مهمان").slice(0, 24),
        sessionId: String(m.sessionId || "").slice(0, 80),
        micOn: m.micOn !== false,
        camOn: false,
        screenOn: false,
        screenAudio: false,
        joinedAt: exists ? this.participants[pid].joinedAt : Date.now(),
        lastSeen: Date.now(),
      };
      try {
        ws.serializeAttachment({ pid });
      } catch {}
      await this.save();
      this.broadcast(this.rosterMsg());
      return;
    }

    if (m.type === "heartbeat") {
      const p = this.participants[m.pid];
      if (p) {
        let changed = false;
        p.lastSeen = Date.now();
        if (typeof m.micOn === "boolean" && m.micOn !== p.micOn) { p.micOn = m.micOn; changed = true; }
        if (typeof m.camOn === "boolean" && m.camOn !== p.camOn) { p.camOn = m.camOn; changed = true; }
        if (typeof m.screenOn === "boolean" && m.screenOn !== p.screenOn) { p.screenOn = m.screenOn; changed = true; }
        if (typeof m.screenAudio === "boolean" && m.screenAudio !== p.screenAudio) { p.screenAudio = m.screenAudio; changed = true; }
        const sid = String(m.sessionId || "").slice(0, 80);
        if (sid && sid !== p.sessionId) { p.sessionId = sid; changed = true; }
        await this.save();
        // مهم: با تغییر وضعیت (مثلاً شروع شیراسکرین) روستر فوراً به همه اطلاع داده شود
        if (changed) this.broadcast(this.rosterMsg());
      }
      await this.purge();
      return;
    }

    if (m.type === "leave") {
      if (this.participants[m.pid]) {
        delete this.participants[m.pid];
        await this.save();
        this.broadcast(this.rosterMsg());
      }
      return;
    }

    // ---------- چت متنی ----------
    if (m.type === "chat") {
      const p = senderPid ? this.participants[senderPid] : null;
      if (!p) return;
      const text = String(m.text ?? "").slice(0, MAX_CHAT_LEN).trim();
      if (!text) return;
      this.broadcast({ type: "chat", pid: p.pid, name: p.name, text, ts: Date.now() }, ws);
      return;
    }

    // ---------- رله فایل ----------
    if (m.type === "file-start") {
      const p = senderPid ? this.participants[senderPid] : null;
      if (!p) return;
      const fileId = String(m.fileId || "").replace(/[^A-Za-z0-9_]/g, "").slice(0, 64);
      const totalChunks = Math.max(1, Math.min(4096, Number(m.totalChunks) || 1));
      if (!fileId) return;
      this.broadcast({
        type: "file-start",
        sender: p.name,
        fileId,
        fileName: String(m.fileName || "file").slice(0, MAX_FILE_NAME),
        fileSize: Math.max(0, Math.min(25 * 1024 * 1024, Number(m.fileSize) || 0)),
        mimeType: String(m.mimeType || "application/octet-stream").slice(0, 100),
        totalChunks,
        time: Date.now(),
      }, ws);
      return;
    }

    if (m.type === "file-chunk") {
      const p = senderPid ? this.participants[senderPid] : null;
      if (!p) return;
      const fileId = String(m.fileId || "").replace(/[^A-Za-z0-9_]/g, "").slice(0, 64);
      const idx = Math.max(0, Math.min(4095, Number(m.chunkIndex) || 0));
      const data = typeof m.data === "string" ? m.data.slice(0, MAX_CHUNK_LEN) : "";
      if (!fileId || !data) return;
      this.broadcast({ type: "file-chunk", fileId, chunkIndex: idx, data }, ws);
      return;
    }

    if (m.type === "file-end") {
      const p = senderPid ? this.participants[senderPid] : null;
      if (!p) return;
      const fileId = String(m.fileId || "").replace(/[^A-Za-z0-9_]/g, "").slice(0, 64);
      if (!fileId) return;
      this.broadcast({ type: "file-end", fileId }, ws);
      return;
    }
  }

  async webSocketClose(ws) {
    const pid = this.pidOf(ws);
    if (!pid) return;
    await this.load();
    if (this.participants[pid]) {
      delete this.participants[pid];
      await this.save();
      this.broadcast(this.rosterMsg());
    }
  }

  async webSocketError(ws) {
    // مثل close رفتار می‌کنیم
    try {
      await this.webSocketClose(ws);
    } catch {}
  }

  async alarm() {
    await this.purge();
  }
}
