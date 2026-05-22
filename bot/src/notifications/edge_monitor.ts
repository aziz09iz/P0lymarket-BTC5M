import type { Bot } from "grammy";
import type { Redis } from "ioredis";
import { getConfig } from "../redis/client.js";
import { trendEmoji, escapeHtml } from "../ui/format.js";

const POLL_INTERVAL_MS = 30_000;

interface EdgeData {
  market_id: string;
  question: string;
  poly_yes_pct: number;
  poly_no_pct: number;
  divergence_score: number;
  expected_repricing: number;
  edge_pct: number;
  tradeable: boolean;
  direction: string;
  btc_price: number;
  btc_trend: string;
  velocity_trend: string;
  time_remaining_secs: number;
  confidence: number;
  price_velocity: number;
  volume_delta: number;
  missing_reason?: string;
  threshold_mode?: string;
  mins_since_last_trade?: number;
  active_min_edge_pct?: number;
  active_min_confidence?: number;
  ts_ms?: number;
}

export function startEdgeMonitor(
  bot: Bot,
  redis: Redis,
  chatIds: number[]
): void {
  if (chatIds.length === 0) return;

  setInterval(async () => {
    try {
      const [
        edgeRaw,
        mode,
        paused,
        minEdgeStr,
        minConfStr,
        exitBeforeFinalStr,
      ] = await Promise.all([
        redis.get("polytrade:edge:snapshot"),
        getConfig(redis, "polytrade:config:mode").then((v) => v ?? "paper"),
        getConfig(redis, "polytrade:config:paused").then((v) => v === "true"),
        redis.get("polytrade:config:min_edge_pct"),
        redis.get("polytrade:config:min_confidence"),
        redis.get("polytrade:config:exit_before_final_secs"),
      ]);

      let message: string;
      const modeStr = mode === "live" ? "🔴 LIVE" : "🟡 PAPER";

      if (!edgeRaw) {
        message = [
          `📡 <b>Edge Monitor</b> · ${modeStr}`,
          `──────────────────────`,
          `Status: ⚪ Engine Offline`,
          `<i>Waiting for Rust core engine…</i>`,
        ].join("\n");
      } else {
        const data: EdgeData = JSON.parse(edgeRaw);

        if (!data.market_id || data.btc_price === 0) {
          // Engine running but no active BTC market
          const trendIcon = trendEmoji(data.btc_trend ?? "Choppy");
          message = [
            `📡 <b>Edge Monitor</b> · ${modeStr}`,
            `──────────────────────`,
            `Status: ⏳ WAITING`,
            `<i>Waiting for active prediction market…</i>`,
            `──────────────────────`,
            `BTC: <b>$${Number(data.btc_price ?? 0).toLocaleString()}</b> ${trendIcon}`,
          ].join("\n");
        } else {
          // Parse config thresholds
          const minEdgePct = minEdgeStr ? parseFloat(minEdgeStr) / 100 : 0.06;
          const minConfidence = minConfStr ? parseFloat(minConfStr) : 0.40;
          const exitBeforeFinalSecs = exitBeforeFinalStr ? parseInt(exitBeforeFinalStr, 10) : 30;

          // Compute values
          const yesPricePct = data.poly_yes_pct * 100;
          const noPricePct = data.poly_no_pct * 100;

          const bestDir = data.direction === "YES" || data.direction === "NO" ? data.direction : "YES";

          const getDivergenceLabel = (v: number) => {
            if (v >= 0.70) return "STRONG";
            if (v >= 0.50) return "OK";
            if (v >= 0.35) return "WEAK";
            return "SKIP";
          };
          const divLabel = getDivergenceLabel(data.divergence_score);

          const getConfidenceLabel = (v: number) => {
            if (v >= 0.70) return "STRONG";
            if (v >= 0.50) return "OK";
            if (v >= 0.35) return "WEAK";
            return "SKIP";
          };
          const getConfidenceBadge = (v: number) => {
            if (v >= 0.50) return "✅";
            if (v >= 0.35) return "⚠️";
            return "❌";
          };
          const confLabel = `${getConfidenceLabel(data.confidence)} ${getConfidenceBadge(data.confidence)}`;

          // Status line logic
          let statusLine = "⏳ WAITING";
          if (paused) {
            statusLine = "⏸ PAUSED";
          } else if (data.time_remaining_secs <= exitBeforeFinalSecs && data.time_remaining_secs > 0) {
            statusLine = `🚫 FINAL WINDOW — no trades`;
          } else if (data.tradeable) {
            statusLine = `🟢 TRADEABLE — signal detected!`;
          } else if (data.missing_reason && data.missing_reason !== "None") {
            statusLine = `⏳ WAITING — ${escapeHtml(data.missing_reason)}`;
          }

          const trendIcon = trendEmoji(data.btc_trend);
          const velStr = data.price_velocity >= 0
            ? `+${data.price_velocity.toFixed(2)}`
            : data.price_velocity.toFixed(2);
          const volDeltaStr = data.volume_delta >= 0
            ? `+${data.volume_delta.toLocaleString()}`
            : data.volume_delta.toLocaleString();

          // Condition checks for the trigger line
          const velOk = Math.abs(data.price_velocity) >= 0.15;
          const velocityDirection = data.price_velocity > 0 ? 1 : (data.price_velocity < 0 ? -1 : 0);
          const deltaDirection = data.volume_delta > 0 ? 1 : (data.volume_delta < 0 ? -1 : 0);
          const deltaAligned = velocityDirection !== 0 && velocityDirection === deltaDirection;
          const spreadOkCheck = !data.missing_reason || !data.missing_reason.toLowerCase().includes("spread");

          const thresholdMode = data.threshold_mode ?? "NORMAL";
          const modeEmoji =
            thresholdMode === "FLOOR" ? "🔴" : thresholdMode === "RELAXED" ? "🟡" : "🟢";
          const minsNoTrade = data.mins_since_last_trade ?? 0;
          const activeEdgePct = (data.active_min_edge_pct ?? minEdgePct) * 100;
          const activeConf = data.active_min_confidence ?? minConfidence;

          const msgLines = [
            `🔍 Edge Monitor · ${modeStr} · ${modeEmoji} ${thresholdMode} MODE`,
            `━━━━━━━━━━━━━━━━━━━━━━━━`,
            `Market:   <code>${escapeHtml(data.market_id)}</code>`,
            `⏱  <code>${data.time_remaining_secs}s</code> remaining · ACTIVE`,
            ``,
            `YES:  <code>${yesPricePct.toFixed(1)}%</code>  NO:  <code>${noPricePct.toFixed(1)}%</code>`,
            minsNoTrade > 0
              ? `⏳ <code>${minsNoTrade} min</code> since last trade`
              : ``,
            ``,
            `BTC signal:  vel <code>${velStr}/s</code> ${trendIcon} · delta <code>${volDeltaStr}</code> · trend: ${data.btc_trend} (${data.velocity_trend})`,
            `Divergence:  <code>${data.divergence_score.toFixed(2)} (${divLabel})</code>`,
            ``,
            `Direction:  ${bestDir === "YES" ? "⬆️ YES" : "⬇️ NO"} (${bestDir === "YES" ? "BTC moving up" : "BTC moving down"}, market still ${bestDir === "YES" ? yesPricePct.toFixed(1) : noPricePct.toFixed(1)}%)`,
            `Est. repricing: <code>+${(data.expected_repricing * 100).toFixed(1)}%</code>`,
            `Confidence:  <code>${data.confidence.toFixed(2)} (${confLabel})</code>`,
            `Status:  <b>${statusLine}</b>`,
            ``,
            `Next trigger at: vel &gt;= 0.15/s ${velOk ? "✅" : "❌"} · delta alignment ${deltaAligned ? "✅" : "❌"} · spread ${spreadOkCheck ? "✅" : "❌"}`,
            `Mode: <code>${thresholdMode}</code> · thresholds: edge &gt;= <code>${activeEdgePct.toFixed(0)}%</code> conf &gt;= <code>${activeConf.toFixed(2)}</code>`,
          ];

          if (data.missing_reason && data.missing_reason !== "None" && !data.tradeable && !paused) {
            msgLines.push(`Missing: ${escapeHtml(data.missing_reason)}`);
          }

          message = msgLines.join("\n");
        }
      }

      // Send to all allowed users
      for (const chatId of chatIds) {
        await bot.api
          .sendMessage(chatId, message, { parse_mode: "HTML" })
          .catch((err: Error) =>
            console.error(`[edge-monitor] send error to ${chatId}: ${err.message}`)
          );
      }
    } catch (err) {
      console.error("[edge-monitor] error:", (err as Error).message);
    }
  }, POLL_INTERVAL_MS);

  console.log("[edge-monitor] started — broadcasting every 30s");
}
