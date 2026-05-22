import type { Bot } from "grammy";
import type { Redis } from "ioredis";
import { getConfig } from "../redis/client.js";
import { telegramNotifications as notify } from "./config.js";
import { escapeHtml } from "../ui/format.js";

function safeStr(val: unknown): string {
  if (val === null || val === undefined) {
    return "";
  }
  return escapeHtml(String(val));
}

// ---------------------------------------------------------------------------
// Real-time notification publisher (minimal — no periodic spam)
// ---------------------------------------------------------------------------

export async function startNotificationPublisher(
  bot: Bot,
  subRedis: Redis,
  cmdRedis: Redis,
  chatIds: number[]
): Promise<void> {
  subRedis.connect().catch((err: Error) => {
    console.warn(
      `[notifications] Redis initial connection failed (${err.message}). Reconnecting in background…`
    );
  });
  subRedis.subscribe("polytrade:events").catch((err: Error) => {
    console.error(`[notifications] Redis subscription failed: ${err.message}`);
  });

  console.log("[notifications] subscription requested for polytrade:events");

  subRedis.on("message", async (_channel: string, message: string) => {
    try {
      const envelope = JSON.parse(message);
      const eventType: string | undefined = envelope.type;
      const data = envelope.data;

      if (!eventType || !data) return;

      const mode = await getConfig(cmdRedis, "polytrade:config:mode").catch(() => "paper");
      const modeLabel = mode === "live" ? "🔴 LIVE" : "🟡 PAPER";
      const messagesToSend: string[] = [];

      switch (eventType) {
        case "paper_open": {
          if (!notify.position_opened) break;

          const profitTargetStr = await cmdRedis
            .get("polytrade:config:profit_target_pct")
            .catch(() => null);
          const stopLossStr = await cmdRedis
            .get("polytrade:config:stop_loss_pct")
            .catch(() => null);
          const exitBeforeFinalStr = await cmdRedis
            .get("polytrade:config:exit_before_final_secs")
            .catch(() => null);

          const profitTargetPct = profitTargetStr ? parseFloat(profitTargetStr) : 12.0;
          const stopLossPct = stopLossStr ? parseFloat(stopLossStr) : 7.0;
          const exitBeforeFinal = exitBeforeFinalStr ? parseInt(exitBeforeFinalStr, 10) : 30;

          const entryPrice = Number(data.entry_price);
          const targetExit = Math.min(0.98, entryPrice + profitTargetPct / 100);
          const stopLossPrice = Math.max(0.02, entryPrice - stopLossPct / 100);

          let timeRemainingSecs = 180;
          const sigRaw = await cmdRedis.get("polytrade:signal:latest").catch(() => null);
          if (sigRaw) {
            try {
              const sig = JSON.parse(sigRaw);
              timeRemainingSecs = sig.time_remaining_secs ?? timeRemainingSecs;
            } catch {
              /* ignore */
            }
          }

          const dirUpper = String(data.direction).toUpperCase();
          const dirLabel = dirUpper === "YES" ? "YES" : "NO";

          messagesToSend.push(
            [
              `📥 Position Opened · ${modeLabel}`,
              `━━━━━━━━━━━━━━━━━━━━━`,
              `${dirLabel} @ ${entryPrice.toFixed(3)} · $${Number(data.size_usd).toFixed(2)} · ${Number(data.share_qty).toFixed(2)} shares`,
              `${safeStr(data.market_id)} · ${timeRemainingSecs}s left`,
              `TP: ${targetExit.toFixed(2)} · SL: ${stopLossPrice.toFixed(2)} · Force: ${exitBeforeFinal}s`,
            ].join("\n")
          );
          break;
        }

        case "paper_close": {
          if (!notify.position_closed) break;

          const stats = await cmdRedis.hgetall("polytrade:paper:stats").catch(() => ({} as Record<string, string>));
          const tradeCount = parseInt(stats.trade_count ?? "0", 10);
          const winCount = parseInt(stats.win_count ?? "0", 10);
          const lossCount = parseInt(stats.loss_count ?? "0", 10);
          const totalPnl = parseFloat(stats.total_pnl ?? "0.0");

          const pnl = parseFloat(data.pnl_usd ?? "0");
          const pnlPct = parseFloat(data.pnl_pct ?? "0");
          const holdSecs = Math.round(Number(data.hold_duration_ms ?? 0) / 1000);
          const isWin = pnl >= 0;
          const resultLabel = isWin ? "✅ WIN" : "❌ LOSS";

          const dirUpper = String(data.direction).toUpperCase();
          const dirLabel = dirUpper === "YES" ? "YES" : "NO";

          let reasonStr = String(data.exit_reason ?? "exit");
          if (reasonStr === "ProfitTarget") reasonStr = "profit target";
          else if (reasonStr === "StopLoss") reasonStr = "stop loss";
          else if (reasonStr === "TimeExit") reasonStr = "time exit";
          else if (reasonStr === "EdgeGone") reasonStr = "edge gone";

          const entryPrice = Number(data.entry_price);
          const exitPrice = Number(data.exit_price);
          const pnlSign = pnl >= 0 ? "+" : "";
          const pnlPctSign = pnlPct >= 0 ? "+" : "";
          const sessionPnlSign = totalPnl >= 0 ? "+" : "";

          messagesToSend.push(
            [
              `📤 Closed · ${modeLabel} · ${resultLabel}`,
              `━━━━━━━━━━━━━━━━━━━━━━`,
              `${dirLabel}  ${entryPrice.toFixed(3)} → ${exitPrice.toFixed(3)}`,
              `PnL: ${pnlSign}$${pnl.toFixed(2)} (${pnlPctSign}${pnlPct.toFixed(1)}%) · hold: ${holdSecs}s`,
              `Reason: ${reasonStr}`,
              ``,
              `Session: ${tradeCount} trades · W:${winCount} L:${lossCount} · ${sessionPnlSign}$${totalPnl.toFixed(2)}`,
            ].join("\n")
          );
          break;
        }

        case "feed_critical": {
          if (!notify.critical_feed_error) break;
          const msg = data.message
            ? safeStr(data.message)
            : `🔴 ${safeStr(data.feed ?? "Feed")} down ${data.down_secs ?? "?"}s+ · REST fallback active`;
          messagesToSend.push(msg.startsWith("🔴") ? msg : `🔴 ${msg}`);
          break;
        }

        case "cycle_event": {
          if (!notify.market_transition) break;
          // Optional — disabled by default
          break;
        }

        case "risk_alert": {
          if (!notify.kill_switch) break;
          const reason = String(data.reason ?? "Risk limit triggered");
          if (
            reason.toLowerCase().includes("kill") ||
            reason.toLowerCase().includes("consecutive") ||
            reason.toLowerCase().includes("cooldown") ||
            reason.toLowerCase().includes("exposure")
          ) {
            messagesToSend.push(`🛑 Kill switch · ${modeLabel}\n${safeStr(reason)}`);
          }
          break;
        }

        default:
          return;
      }

      if (messagesToSend.length > 0) {
        for (const chatId of chatIds) {
          for (const msg of messagesToSend) {
            bot.api
              .sendMessage(chatId, msg, { parse_mode: "HTML" })
              .catch((err: Error) =>
                console.error(`[notifications] send error: ${err.message}`)
              );
          }
        }
      }
    } catch {
      // Not a JSON event we care about — ignore.
    }
  });
}
