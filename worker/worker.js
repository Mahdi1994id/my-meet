// Zumi SFU — Cloudflare Worker
// روت‌ها: صفحه اصلی، ICE (TURN)، پروکسی SFU، سیگنالینگ اتاق (Durable Object)

import UI from "./ui.js";
import { Room } from "./room.js";

const RTC_BASE = "https://rtc.live.cloudflare.com/v1";

const JSON_HEADERS = { "content-type": "application/json; charset=utf-8" };

function json(data, status = 200) {
  return new Response(JSON.stringify(data), { status, headers: JSON_HEADERS });
}

// اعتبارسنجی sessionId (فقط هگز) تا از سوءاستفاده جلوگیری شود
const SID_RE = /^[a-f0-9]{24,80}$/;

async function sfuProxy(env, path, method, body) {
  const r = await fetch(`${RTC_BASE}/apps/${env.SFU_APP_ID}${path}`, {
    method,
    headers: {
      "Authorization": `Bearer ${env.SFU_APP_SECRET}`,
      "Content-Type": "application/json",
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await r.text();
  return new Response(text, { status: r.status, headers: JSON_HEADERS });
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;

    // صفحه اصلی
    if (path === "/" || path === "/index.html") {
      return new Response(UI, {
        headers: { "content-type": "text/html; charset=utf-8", "cache-control": "no-store" },
      });
    }

    // سلامت
    if (path === "/api/health") return json({ ok: true, service: "zumi-sfu" });

    // ICE servers (STUN + TURN کلادفلر با پورت 443/TLS برای اینترنت ایران)
    if (path === "/api/ice" && request.method === "GET") {
      try {
        const r = await fetch(
          `${RTC_BASE}/turn/keys/${env.TURN_KEY_ID}/credentials/generate-ice-servers`,
          {
            method: "POST",
            headers: {
              "Authorization": `Bearer ${env.TURN_KEY_SECRET}`,
              "Content-Type": "application/json",
            },
            body: JSON.stringify({ ttl: 86400 }),
          }
        );
        const data = await r.json();
        if (!data.iceServers) return json({ error: "turn_failed" }, 502);
        return json({ iceServers: data.iceServers });
      } catch (e) {
        return json({ error: "turn_failed", detail: String(e) }, 502);
      }
    }

    // پروکسی SFU — ساخت سشن
    if (path === "/api/sfu/session" && request.method === "POST") {
      return sfuProxy(env, "/sessions/new", "POST");
    }

    const m = path.match(/^\/api\/sfu\/session\/([a-f0-9]+)\/(tracks|renegotiate|tracks-close)$/);
    if (m) {
      const [, sid, op] = m;
      if (!SID_RE.test(sid)) return json({ error: "bad_session" }, 400);
      let body;
      try {
        body = await request.json();
      } catch {
        return json({ error: "bad_json" }, 400);
      }
      if (op === "tracks") return sfuProxy(env, `/sessions/${sid}/tracks/new`, "POST", body);
      if (op === "renegotiate") return sfuProxy(env, `/sessions/${sid}/renegotiate`, "PUT", body);
      if (op === "tracks-close") return sfuProxy(env, `/sessions/${sid}/tracks/close`, "PUT", body);
    }

    // سیگنالینگ اتاق با Durable Object
    const rm = path.match(/^\/api\/room\/([A-Za-z0-9_-]{3,32})\/ws$/);
    if (rm && request.headers.get("Upgrade") === "websocket") {
      const roomId = rm[1].toLowerCase();
      const id = env.ROOMS.idFromName(roomId);
      const stub = env.ROOMS.get(id);
      return stub.fetch(request);
    }

    return json({ error: "not_found" }, 404);
  },
};

export { Room };