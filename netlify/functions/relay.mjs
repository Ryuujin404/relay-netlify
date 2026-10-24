// netlify/functions/relay.mjs
// Port dari relay Vercel v2.6 → Netlify Functions v2 (Request/Response + ReadableStream)
//
// ⚠ Perbedaan platform (sudah diskalakan):
//   - Streaming response: hard limit 60 detik/request (Vercel: 300s) → UPSTREAM_TIMEOUT 55s
//   - Response size: max 20 MB (Vercel: 50MB)
//   - Netlify TIDAK punya config.maxDuration → pakai PLATFORM_BUDGET_MS sebagai gerbang total
//   - Endpoint tetap: /.netlify/functions/relay  (kompatibel dengan 9Router proxyAwareFetch:
//     header x-relay-target + x-relay-path, format error JSON sama persis)

const UPSTREAM_TIMEOUT   = 55_000;  // ⚠ 290s di Vercel → 55s (harus muat di bawah limit streaming 60s)
const CONNECT_TIMEOUT    = 10_000;  // ⚠ 15s → 10s
const INTER_BYTE_TIMEOUT = 30_000;  // ⚠ 45s → 30s
const FIRST_DATA_WAIT_MS = 15_000;  // ⚠ 60s → 15s
const MAX_RETRIES        = 3;
const MAX_BACKOFF_MS     = 4_000;   // ⚠ 8s → 4s (hemat budget waktu)
const PLATFORM_BUDGET_MS = 55_000;  // total waktu seluruh attempt (Netlify hard-kill di 60s)
const CIRCUIT_THRESHOLD  = 3;
const CIRCUIT_COOLDOWN   = 60_000;
const MAX_RESPONSE_SIZE  = 20 * 1024 * 1024; // ⚠ 50MB → 20MB (limit platform)

const STRIP_HEADERS = new Set([
  'x-relay-target', 'x-relay-path', 'host',
  'content-length', 'connection', 'transfer-encoding',
]);

const circuitStates = new Map();

function uid() { return Math.random().toString(36).slice(2, 10); }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function log(reqId, msg) { console.log(`[${reqId}] ${msg}`); }

// ─── Circuit breaker (identik) ───
// Catatan: state ini per-instance lambda hangat — perilaku sama seperti di Vercel.
function getCircuit(hostname) {
  const now = Date.now();
  const s = circuitStates.get(hostname);
  if (!s) return { open: false };
  if (s.failures >= CIRCUIT_THRESHOLD && (now - s.lastFail) < CIRCUIT_COOLDOWN)
    return { open: true, until: s.lastFail + CIRCUIT_COOLDOWN };
  if ((now - s.lastFail) >= CIRCUIT_COOLDOWN) circuitStates.delete(hostname);
  return { open: false };
}

function recordFail(hostname) {
  const e = circuitStates.get(hostname) || { failures: 0, lastFail: 0 };
  e.failures = Math.min(e.failures + 1, CIRCUIT_THRESHOLD);
  e.lastFail = Date.now();
  circuitStates.set(hostname, e);
}

function recordSuccess(hostname) { circuitStates.delete(hostname); }

// ─── Helpers Web API (pengganti res.setHeader) ───
function corsHeaders() {
  return {
    'access-control-allow-origin': '*',
    'access-control-allow-methods': 'GET, POST, OPTIONS',
    'access-control-allow-headers': 'Content-Type, Authorization, x-relay-target, x-relay-path, x-request-id',
  };
}

function jsonRes(obj, status, extra = {}) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { ...corsHeaders(), 'content-type': 'application/json', ...extra },
  });
}

// Copy header upstream ke objek header response, buang hop-by-hop + content-length
function copyResHeaders(response, into) {
  response.headers.forEach((v, k) => {
    const lk = k.toLowerCase();
    if (lk === 'content-length' || lk === 'transfer-encoding' || lk === 'connection') return;
    into[k] = v;
  });
}

export default async function handler(req) {
  const reqId = req.headers.get('x-request-id') || uid();
  const start = Date.now();
  const url = new URL(req.url);

  if (req.method === 'OPTIONS') return new Response(null, { status: 204, headers: corsHeaders() });

  // ─── health: /.netlify/functions/relay/health (atau ?health=1) ───
  if (url.pathname.replace(/\/+$/, '').endsWith('/health') || url.searchParams.has('health')) {
    return jsonRes({
      status: 'ok', uptime: process.uptime(),
      circuits: Object.fromEntries(circuitStates),
      version: '2.6-netlify',
    }, 200);
  }

  const target    = req.headers.get('x-relay-target');
  const relayPath = req.headers.get('x-relay-path') || '/';
  if (!target) return jsonRes({ error: 'Missing x-relay-target', reqId }, 400);

  let targetUrl, hostname;
  try {
    targetUrl = target.replace(/\/$/, '') + relayPath;
    hostname  = new URL(targetUrl).hostname;
  } catch {
    return jsonRes({ error: 'Invalid x-relay-target URL', reqId }, 400);
  }

  const initCirc = getCircuit(hostname);
  if (initCirc.open) {
    const retryAfter = Math.ceil((initCirc.until - Date.now()) / 1000);
    return jsonRes({
      error: 'Service Unavailable',
      message: `${hostname} circuit open`,
      retryAfter, reqId,
    }, 503, { 'retry-after': String(retryAfter) });
  }

  const headers = {};
  req.headers.forEach((v, k) => {
    if (!STRIP_HEADERS.has(k.toLowerCase())) headers[k] = v;
  });
  headers['x-request-id'] = reqId;

  let body;
  if (req.method !== 'GET' && req.method !== 'HEAD') {
    let raw;
    try { raw = await req.text(); }
    catch { return jsonRes({ error: 'Failed to read request body', reqId }, 400); }
    if (raw) {
      try { JSON.parse(raw); body = raw; }
      catch { return jsonRes({ error: 'Invalid JSON body', reqId }, 400); }
    }
  }

  const clientGone = () => req.signal?.aborted === true;

  let lastErr, lastStatus;
  let totalAttempts = 0;

  for (let attempt = 1; attempt <= MAX_RETRIES + 1; attempt++) {
    totalAttempts = attempt;

    if (clientGone()) {
      log(reqId, `⚡ CLIENT_GONE before attempt ${attempt}`);
      return new Response(null, { status: 499 });
    }

    // Gerbang total: jangan biarkan platform nge-kill function di tengah jalan
    if (Date.now() - start > PLATFORM_BUDGET_MS) {
      log(reqId, `⏰ PLATFORM_BUDGET exhausted before attempt ${attempt}`);
      break;
    }

    if (attempt > 1) {
      const circCheck = getCircuit(hostname);
      if (circCheck.open) {
        log(reqId, `🔴 CIRCUIT_OPEN on attempt ${attempt}, stop retrying`);
        lastErr = new Error(`Circuit open for ${hostname}`);
        break;
      }
    }

    const ctrl = new AbortController();
    const onClientAbort = () => ctrl.abort();
    req.signal?.addEventListener('abort', onClientAbort, { once: true });

    const deadline = Date.now() + UPSTREAM_TIMEOUT;
    let totalTimer, connectTimer, phaseTimer;
    const cleanup = () => {
      clearTimeout(totalTimer);
      clearTimeout(connectTimer);
      clearTimeout(phaseTimer);
      req.signal?.removeEventListener('abort', onClientAbort);
    };

    try {
      totalTimer   = setTimeout(() => ctrl.abort(), UPSTREAM_TIMEOUT);
      connectTimer = setTimeout(() => ctrl.abort(), CONNECT_TIMEOUT);

      log(reqId, `🔄 ATTEMPT ${attempt}/${MAX_RETRIES + 1} → ${targetUrl}`);

      const response = await fetch(targetUrl, {
        method: req.method, headers, body, signal: ctrl.signal,
      });
      clearTimeout(connectTimer);

      const cl = response.headers.get('content-length');
      if (cl && parseInt(cl) > MAX_RESPONSE_SIZE) {
        cleanup();
        return jsonRes({ error: 'Response too large', reqId }, 413);
      }

      lastStatus = response.status;

      // ── 429: forward rate limit ──
      if (response.status === 429) {
        cleanup();
        const text = await response.text();
        const h = { ...corsHeaders() };
        copyResHeaders(response, h);
        h['retry-after'] = response.headers.get('retry-after') || '60';
        h['x-proxy-req-id'] = reqId;
        return new Response(text, { status: 429, headers: h });
      }

      // ── 502-504: retry with backoff ──
      if (response.status >= 502 && response.status <= 504) {
        recordFail(hostname);
        cleanup();
        if (attempt <= MAX_RETRIES) {
          const backoff = Math.min(1000 * Math.pow(2, attempt - 1), MAX_BACKOFF_MS);
          log(reqId, `⚠ UPSTREAM_${response.status} → backoff ${backoff}ms`);
          await sleep(backoff);
          continue;
        }
        lastErr = new Error(`Upstream returned ${response.status}`);
        break;
      }

      if (response.status >= 200 && response.status < 300) recordSuccess(hostname);

      // ── 4xx client error: forward as-is ──
      if (response.status >= 400 && response.status < 500) {
        cleanup();
        const text = await response.text();
        const h = { ...corsHeaders() };
        copyResHeaders(response, h);
        h['x-proxy-req-id'] = reqId;
        return new Response(text, { status: response.status, headers: h });
      }

      // ── 2xx: TWO-PHASE STREAMING (versi Web API) ──
      if (response.body) {
        const reader = response.body.getReader();

        // ── PHASE 1: tunggu first body bytes (BELUM mengirim apa pun ke client) ──
        let firstChunk;
        try {
          const first = await Promise.race([
            reader.read(),
            new Promise((_, reject) => {
              phaseTimer = setTimeout(() => {
                const e = new Error('FIRST_DATA_TIMEOUT');
                e.code = 'FIRST_DATA_TIMEOUT';
                reject(e);
              }, FIRST_DATA_WAIT_MS);
            }),
          ]);
          clearTimeout(phaseTimer);
          if (first.done) {
            const e = new Error('STREAM_EMPTY');
            e.code = 'STREAM_EMPTY';
            throw e;
          }
          firstChunk = first.value;
        } catch (phaseErr) {
          // Body kosong / timeout → AMAN untuk retry karena belum ada response dikirim
          cleanup(); recordFail(hostname);
          try { await reader.cancel(); } catch {}
          const reason = phaseErr.code || phaseErr.message;
          log(reqId, `⚠ ${reason} attempt ${attempt} — 0 body bytes, retrying`);
          if (attempt <= MAX_RETRIES && !clientGone()) {
            const backoff = Math.min(1000 * Math.pow(2, attempt - 1), MAX_BACKOFF_MS);
            log(reqId, `🔁 RETRY ${attempt + 1} in ${backoff}ms`);
            await sleep(backoff);
            continue; // ✅ RETRY AMAN
          }
          lastErr = phaseErr;
          break;
        }

        // ── PHASE 2: first chunk tiba → baru commit headers + stream sisanya ──
        log(reqId, `📥 FIRST_BYTE after ${Date.now() - start}ms`);

        const h = { ...corsHeaders() };
        copyResHeaders(response, h);
        h['cache-control'] = 'no-cache, no-transform';
        h['x-accel-buffering'] = 'no';
        h['x-proxy-req-id'] = reqId;

        let pending = firstChunk;
        const stream = new ReadableStream({
          async pull(controller) {
            try {
              if (pending) { controller.enqueue(pending); pending = null; return; }

              const remaining = deadline - Date.now();
              if (remaining <= 0) {
                log(reqId, `⏱ UPSTREAM_TOTAL_TIMEOUT`);
                cleanup();
                try { await reader.cancel(); } catch {}
                controller.close(); // sudah streaming → tidak bisa retry, tutup rapi (≈ res.end())
                return;
              }

              // inter-byte stall guard: read() dilombakan dengan timer
              let stallFired = false;
              const stall = new Promise((resolve) => {
                phaseTimer = setTimeout(() => { stallFired = true; resolve(); },
                  Math.min(INTER_BYTE_TIMEOUT, remaining));
              });

              let result;
              try { result = await Promise.race([reader.read(), stall]); }
              catch (readErr) {
                clearTimeout(phaseTimer);
                log(reqId, `❌ ERROR mid-stream: ${readErr.message}`);
                cleanup();
                try { await reader.cancel(); } catch {}
                controller.close();
                return;
              }
              clearTimeout(phaseTimer);

              if (stallFired) {
                log(reqId, `⏱ INTER_BYTE_STALL`);
                cleanup();
                try { await reader.cancel(); } catch {}
                controller.close();
                return;
              }
              if (result.done) {
                cleanup();
                log(reqId, `✅ DONE ${response.status} in ${Date.now() - start}ms`);
                controller.close();
                return;
              }
              controller.enqueue(result.value);
            } catch (err) {
              cleanup();
              log(reqId, `❌ ERROR mid-stream (${Date.now() - start}ms): ${err.message}`);
              try { controller.close(); } catch {}
            }
          },
          cancel(reason) {
            log(reqId, `⚡ CLIENT_CLOSED_STREAM`);
            cleanup();
            try { reader.cancel(reason); } catch {}
          },
        });

        return new Response(stream, { status: response.status, headers: h });
      }

      // No body (HEAD, dll)
      cleanup();
      return new Response(null, { status: response.status, headers: corsHeaders() });

    } catch (err) {
      cleanup();
      lastErr = err;
      recordFail(hostname);

      const retryable =
        err.name === 'AbortError' ||
        ['ECONNRESET', 'ETIMEDOUT', 'ECONNREFUSED', 'EPIPE', 'ENOTFOUND'].includes(err.code) ||
        (err.message || '').includes('fetch failed');

      if (retryable && attempt <= MAX_RETRIES && !clientGone()) {
        const backoff = Math.min(1000 * Math.pow(2, attempt - 1), MAX_BACKOFF_MS);
        const reason = err.name === 'AbortError' ? 'timeout' : (err.code || err.message);
        log(reqId, `⚠ ${reason} → backoff ${backoff}ms → retry ${attempt + 1}`);
        await sleep(backoff);
        continue; // (typo `continue;a` di script Vercel lo sudah diperbaiki 😄)
      }
      break;
    }
  }

  log(reqId, `❌ FAILED after ${totalAttempts} attempt(s) in ${Date.now() - start}ms`);
  const code = lastStatus || (lastErr?.name === 'AbortError' ? 504 : 502);
  return jsonRes({
    error: code === 504 ? 'Gateway Timeout' : 'Bad Gateway',
    message: lastErr?.message || 'Upstream failed',
    attempts: totalAttempts, reqId,
  }, code);
}
