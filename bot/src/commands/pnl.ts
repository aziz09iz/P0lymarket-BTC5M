import type { Context } from "grammy";
import type { Redis } from "ioredis";
import { getConfig } from "../redis/client.js";
import { SEP } from "../ui/format.js";

export async function handlePnl(ctx: Context, redis: Redis): Promise<void> {
  try {
    const mode = (await getConfig(redis, "polytrade:config:mode")) ?? "paper";
    const modeLabel = mode === "live" ? "🔴 LIVE" : "🟡 PAPER";

    const [stats, suspiciousRaw] = await Promise.all([
      redis.hgetall("polytrade:paper:stats"),
      redis.lrange("polytrade:paper:suspicious_trades", 0, 9),
    ]);

    const tradeCount = parseInt(stats.trade_count ?? "0", 10);
    const suspiciousCount = parseInt(stats.suspicious_count ?? "0", 10);

    if (tradeCount === 0 && suspiciousCount === 0 && (!stats || Object.keys(stats).length === 0)) {
      await ctx.reply(
        [
          `💰 <b>PnL Summary · ${modeLabel}</b>`,
          SEP,
          `No trades recorded yet.`,
        ].join("\n"),
        { parse_mode: "HTML" }
      );
      return;
    }

    const totalPnl = parseFloat(stats.total_pnl ?? "0.0");
    const winCount = parseInt(stats.win_count ?? "0", 10);
    const lossCount = parseInt(stats.loss_count ?? "0", 10);

    const winRate = tradeCount > 0 ? (winCount / tradeCount) * 100 : 0.0;
    const avgWin = parseFloat(stats.avg_win ?? "0.0");
    const avgLoss = parseFloat(stats.avg_loss ?? "0.0");
    const bestTrade = parseFloat(stats.best_trade ?? "0.0");
    const worstTrade = parseFloat(stats.worst_trade ?? "0.0");

    const edgeAvg = parseFloat(stats.edge_avg ?? "0.0");
    const avgHoldSecs = parseFloat(stats.avg_hold_secs ?? "0.0");

    const yesTrades = parseInt(stats.yes_trades ?? "0", 10);
    const yesWins = parseInt(stats.yes_wins ?? "0", 10);
    const yesWr = yesTrades > 0 ? (yesWins / yesTrades) * 100 : 0.0;

    const noTrades = parseInt(stats.no_trades ?? "0", 10);
    const noWins = parseInt(stats.no_wins ?? "0", 10);
    const noWr = noTrades > 0 ? (noWins / noTrades) * 100 : 0.0;

    const consecutiveLosses = parseInt(stats.consecutive_losses ?? "0", 10);
    const cooldownUntilMs = parseInt(stats.cooldown_until_ms ?? "0", 10);
    const nowMs = Date.now();
    let cooldownStr = "none";
    if (cooldownUntilMs > nowMs) {
      const remainingSecs = Math.ceil((cooldownUntilMs - nowMs) / 1000);
      cooldownStr = `${remainingSecs}s remaining`;
    }

    const formatPnl = (val: number) => {
      const sign = val >= 0 ? "+" : "-";
      return `<code>${sign}$${Math.abs(val).toFixed(2)}</code>`;
    };

    const lines = [
      `💰 <b>PnL Summary · ${modeLabel}</b>`,
      SEP,
      `Valid trades:      <code>${tradeCount}</code> | W: <code>${winCount}</code> L: <code>${lossCount}</code> WR: <code>${winRate.toFixed(0)}%</code>`,
    ];

    if (suspiciousCount > 0) {
      lines.push(`Suspicious trades: <code>${suspiciousCount}</code> ⚠️ (excluded from stats)`);
      const parsedSuspicious = (suspiciousRaw || [])
        .map((r) => {
          try {
            return JSON.parse(r);
          } catch {
            return null;
          }
        })
        .filter((t) => t !== null);

      const latestSuspicious = parsedSuspicious.slice(0, 5);
      for (const t of latestSuspicious) {
        const holdSecs = Math.round(t.hold_duration_ms / 1000);
        const reasonStr = t.suspicious_reason ?? "stale data artifact";
        const sign = t.pnl_pct >= 0 ? "+" : "";
        const dirLabel = t.direction ? t.direction.toUpperCase() : "—";
        lines.push(`  └ ${dirLabel} @ ${t.entry_price.toFixed(3)}→${t.exit_price.toFixed(3)} in ${holdSecs}s (${sign}${t.pnl_pct.toFixed(0)}%) — ${reasonStr}`);
      }
    } else {
      lines.push(`Suspicious trades: <code>0</code>`);
    }

    const avgPnlPerTrade = tradeCount > 0 ? totalPnl / tradeCount : 0.0;
    const stalePriceBug =
      tradeCount >= 2 && Math.abs(totalPnl) < 0.001 && Math.abs(avgPnlPerTrade) < 0.001;

    lines.push("");
    if (stalePriceBug) {
      lines.push(
        `⚠️ <b>Real PnL: $0.00 — likely stale price feed</b>`,
        `   All exits at same price as entry.`,
        `   Check: <code>redis-cli get polytrade:btc5m:yes_price</code> (should change every few seconds)`
      );
    } else {
      lines.push(`Real PnL:       ${formatPnl(totalPnl)}`);
    }

    if (stalePriceBug) {
      lines.push(`Real PnL:       ${formatPnl(totalPnl)} (suspect — see warning above)`);
    }

    lines.push(
      `Avg win:        ${formatPnl(avgWin)}`,
      `Avg loss:       ${formatPnl(avgLoss)}`,
      `Best trade:     ${formatPnl(bestTrade)}`,
      `Worst trade:    ${formatPnl(worstTrade)}`,
      "",
      `Edge avg:       <code>${edgeAvg >= 0 ? "+" : ""}${(edgeAvg * 100).toFixed(1)}%</code>`,
      `Avg hold:       <code>${avgHoldSecs.toFixed(0)}s</code>`,
      "",
      `Direction breakdown:`,
      `  YES trades: <code>${yesTrades}</code>  W: <code>${yesWins}</code>  WR: <code>${yesWr.toFixed(1)}%</code>`,
      `  NO trades:  <code>${noTrades}</code>  W: <code>${noWins}</code>  WR: <code>${noWr.toFixed(1)}%</code>`,
      "",
      `Consecutive losses now: <code>${consecutiveLosses}</code>`,
      `Cooldown: <code>${cooldownStr}</code>`
    );

    await ctx.reply(lines.join("\n"), { parse_mode: "HTML" });
  } catch (err) {
    await ctx.reply(`❌ Error: ${(err as Error).message}`);
  }
}
