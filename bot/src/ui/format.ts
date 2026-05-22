// ---------------------------------------------------------------------------
// Pro UI formatting utilities for PolyTrade Telegram Bot
// ---------------------------------------------------------------------------

/** Format a UTC timestamp as "Xm Ys ago" */
export function ago(tsMs: number): string {
  const diff = Date.now() - tsMs;
  if (diff < 0) return "just now";
  const s = Math.floor(diff / 1000);
  if (s < 60) return `${s}s ago`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${s % 60}s ago`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m ago`;
}

/** Format uptime from a start_time_ms value */
export function uptime(startMs: number): string {
  const diff = Date.now() - startMs;
  const s = Math.floor(diff / 1000);
  const m = Math.floor(s / 60);
  const h = Math.floor(m / 60);
  const d = Math.floor(h / 24);
  if (d > 0) return `${d}d ${h % 24}h ${m % 60}m`;
  if (h > 0) return `${h}h ${m % 60}m`;
  if (m > 0) return `${m}m ${s % 60}s`;
  return `${s}s`;
}

/** Format USD value — always shows sign for PnL */
export function usd(n: number, showSign = false): string {
  const sign = showSign ? (n >= 0 ? "+" : "-") : n < 0 ? "-" : "";
  return `${sign}$${Math.abs(n).toFixed(2)}`;
}

/** Format a percentage (0.0–1.0 range → "57.3%") */
export function pct(n: number): string {
  return `${(n * 100).toFixed(1)}%`;
}

/** Format an edge percentage with sign and 🔥 for high edge */
export function edgePct(n: number): string {
  const sign = n >= 0 ? "+" : "";
  const val = `${sign}${(n * 100).toFixed(1)}%`;
  if (Math.abs(n) >= 0.10) return `${val} 🔥`;
  if (Math.abs(n) >= 0.05) return `${val} ⚡`;
  return val;
}

/** Trend emoji based on microtrend string */
export function trendEmoji(trend: string): string {
  switch (trend?.toLowerCase()) {
    case "bullish": return "📈";
    case "bearish": return "📉";
    default: return "↔️";
  }
}

/** Mode badge */
export function modeBadge(mode: string): string {
  return mode === "live" ? "🔴 LIVE" : "🟡 PAPER";
}

/** State badge */
export function stateBadge(paused: boolean): string {
  return paused ? "⏸️ Paused" : "✅ Running";
}

/** Tradeable status badge */
export function tradeableBadge(tradeable: boolean): string {
  return tradeable ? "✅ <b>TRADEABLE</b>" : "⏳ WAITING";
}

/** Latency badge — color-coded */
export function latencyBadge(ms: number | string): string {
  const n = typeof ms === "string" ? parseInt(ms, 10) : ms;
  if (isNaN(n)) return "—";
  if (n < 100) return `🟢 ${n}ms`;
  if (n < 300) return `🟡 ${n}ms`;
  return `🔴 ${n}ms`;
}

/** Shorten a market ID for display: "0x1234...abcd" */
export function shortId(id: string): string {
  if (!id || id.length <= 12) return id;
  return `${id.slice(0, 6)}…${id.slice(-4)}`;
}

/** Separator line */
export const SEP = "━━━━━━━━━━━━━━━━━━━━━━";
export const SEP_THIN = "──────────────────────";

/** Section header */
export function header(icon: string, title: string): string {
  return `${icon} <b>${title}</b>`;
}

/** Build the beautiful edge monitor card */
export function buildEdgeCard(
  data: {
    question: string;
    poly_yes_pct: number;
    internal_yes_pct: number;
    edge_pct: number;
    tradeable: boolean;
    direction: string;
    btc_price: number;
    btc_trend: string;
    ts_ms?: number;
  },
  mode: string
): string {
  const trend = trendEmoji(data.btc_trend);
  const modeStr = mode === "live" ? "🔴 LIVE" : "🟡 PAPER";
  
  // Status Badge
  const statusStr = data.tradeable ? "🟢 TRADEABLE" : "⏳ WAITING";
  
  // Edge Formatting
  const edgeVal = (data.edge_pct * 100).toFixed(1);
  const edgeSign = data.edge_pct >= 0 ? "+" : "";
  const edgeAbs = Math.abs(data.edge_pct);
  let edgeEmoji = "📊";
  if (edgeAbs >= 0.10) edgeEmoji = "🔥🔥";
  else if (edgeAbs >= 0.05) edgeEmoji = "🔥";

  // Shortened question
  const q = data.question.length > 45
    ? data.question.slice(0, 44) + "…"
    : data.question || "No active BTC market";

  const lines = [
    `📡 <b>EDGE MONITOR · BTC 5M</b> · ${modeStr}`,
    `──────────────────────`,
    `<b>Market:</b> <i>${q}</i>`,
    `──────────────────────`,
    `<code>Polymarket YES : ${pct(data.poly_yes_pct).padEnd(8)}</code>`,
    `<code>Internal YES   : ${pct(data.internal_yes_pct).padEnd(8)}</code>`,
    ``,
    `<code>Edge           : ${edgeSign}${edgeVal}% (${data.direction})</code> ${edgeEmoji}`,
    `<code>Status         : ${statusStr}</code>`,
    `──────────────────────`,
    `BTC: <b>$${Number(data.btc_price).toLocaleString()}</b> ${trend}`,
  ];

  if (data.ts_ms) {
    lines.push(`<i>${ago(data.ts_ms)}</i>`);
  }

  return lines.join("\n");
}

/** Build waiting card (no active markets) */
export function buildWaitingCard(btcPrice: number, btcTrend: string, mode: string): string {
  const trend = trendEmoji(btcTrend);
  const modeStr = mode === "live" ? "🔴 LIVE" : "🟡 PAPER";
  return [
    `📡 <b>Edge Monitor</b> · ${modeStr}`,
    `──────────────────────`,
    `Status: ⏳ WAITING`,
    `<i>Waiting for active prediction market…</i>`,
    `──────────────────────`,
    `BTC: <b>$${Number(btcPrice).toLocaleString()}</b> ${trend}`,
  ].join("\n");
}
