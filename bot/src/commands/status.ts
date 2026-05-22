import type { Context } from "grammy";
import type { Redis } from "ioredis";
import { trendEmoji, usd, SEP, header } from "../ui/format.js";
import { getConfig } from "../redis/client.js";

export async function handleStatus(ctx: Context, redis: Redis): Promise<void> {
  try {
    const [
      signalRaw,
      btcRaw,
      mode,
      paused,
      posRaw,
      minEdgeStr,
      minConfStr,
      profitTargetStr,
      exitBeforeFinalStr,
    ] = await Promise.all([
      redis.get("polytrade:signal:latest"),
      redis.get("polytrade:state:btc"),
      getConfig(redis, "polytrade:config:mode").then((v) => v ?? "paper"),
      getConfig(redis, "polytrade:config:paused").then((v) => v === "true"),
      redis.get("polytrade:paper:positions"),
      redis.get("polytrade:config:min_edge_pct"),
      redis.get("polytrade:config:min_confidence"),
      redis.get("polytrade:config:profit_target_pct"),
      redis.get("polytrade:config:exit_before_final_secs"),
    ]);

    const modeLabel = mode === "live" ? "🔴 LIVE" : "🟡 PAPER";
    const title = `🤖 PolyTrade 5M · ${modeLabel}`;

    // Defaults
    const minEdgePct = minEdgeStr ? parseFloat(minEdgeStr) / 100 : 0.08;
    const minConfidence = minConfStr ? parseFloat(minConfStr) : 0.45;
    const profitTargetPct = profitTargetStr ? parseFloat(profitTargetStr) : 15.0;
    const exitBeforeFinalSecs = exitBeforeFinalStr ? parseInt(exitBeforeFinalStr, 10) : 30;

    let marketId = "—";
    let timeRemainingSecs = 0;
    let yesPrice = 0.0;
    let noPrice = 0.0;
    let divergenceScore = 0.0;
    let expectedRepricing = 0.0;
    let edgePct = 0.0;
    let tradeable = false;
    let direction = "—";
    let confidence = 0.0;
    let btcPrice = 0.0;
    let priceVelocity = 0.0;
    let btcTrend = "Choppy";
    let velocityTrend = "Stable";
    let volumeDelta = 0.0;
    let binanceLat: number | undefined;
    let polyLat: number | undefined;
    let clockOffset: number | undefined;
    let missingReason = "";

    // Data quality / health additions
    let binancePing: number | undefined;
    let binanceTickRate = 0.0;
    let polyTickRate = 0.0;
    let binanceLastMsgMs: number | undefined;
    let polyLastMsgMs: number | undefined;
    let priceRejections = 0;
    let fallbackMode = false;
    let polyLastFetchedMs: number | undefined;
    let thresholdMode = "NORMAL";
    let minsSinceLastTrade = 0;
    let activeMinEdgePct = minEdgePct;
    let activeMinConfidence = minConfidence;

    if (signalRaw) {
      const sig = JSON.parse(signalRaw);
      marketId = sig.market_id ?? "—";
      timeRemainingSecs = sig.time_remaining_secs ?? 0;
      yesPrice = sig.poly_yes_pct ?? 0.0;
      noPrice = sig.poly_no_pct ?? 0.0;
      divergenceScore = sig.divergence_score ?? 0.0;
      expectedRepricing = sig.expected_repricing ?? 0.0;
      edgePct = sig.edge_pct ?? 0.0;
      tradeable = sig.tradeable ?? false;
      direction = sig.direction ?? "—";
      confidence = sig.confidence ?? 0.0;
      btcPrice = sig.btc_price ?? 0.0;
      priceVelocity = sig.price_velocity ?? 0.0;
      btcTrend = sig.btc_trend ?? "Choppy";
      velocityTrend = sig.velocity_trend ?? "Stable";
      volumeDelta = sig.volume_delta ?? 0.0;
      binanceLat = sig.binance_latency_ms;
      polyLat = sig.polymarket_latency_ms;
      clockOffset = sig.clock_offset_ms;
      missingReason = sig.missing_reason ?? "";

      binancePing = sig.binance_ping_ms;
      binanceTickRate = sig.binance_tick_rate ?? 0.0;
      polyTickRate = sig.polymarket_tick_rate ?? 0.0;
      binanceLastMsgMs = sig.binance_last_msg_ms;
      polyLastMsgMs = sig.polymarket_last_msg_ms;
      priceRejections = sig.price_rejections ?? 0;
      fallbackMode = sig.fallback_mode ?? false;
      polyLastFetchedMs = sig.poly_last_fetched_ms;
      thresholdMode = sig.threshold_mode ?? "NORMAL";
      minsSinceLastTrade = sig.mins_since_last_trade ?? 0;
      activeMinEdgePct = sig.active_min_edge_pct ?? minEdgePct;
      activeMinConfidence = sig.active_min_confidence ?? minConfidence;
    }

    if (btcRaw) {
      const btc = JSON.parse(btcRaw);
      if (btcPrice === 0.0) btcPrice = btc.price ?? 0.0;
      if (priceVelocity === 0.0) priceVelocity = btc.price_velocity ?? 0.0;
      if (btcTrend === "Choppy") btcTrend = btc.microtrend ?? "Choppy";
      if (velocityTrend === "Stable") velocityTrend = btc.velocity_trend ?? "Stable";
      volumeDelta = btc.volume_delta ?? 0.0;
    }

    const positions = posRaw ? JSON.parse(posRaw) : [];
    const hasPosition = Array.isArray(positions) && positions.length > 0;

    // Edge helper
    const bestDir = direction === "YES" || direction === "NO" ? direction : "YES";
    const bestPrice = bestDir === "YES" ? yesPrice : noPrice;

    // Label classification
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

    const getDivergenceLabel = (v: number) => {
      if (v >= 0.70) return "STRONG";
      if (v >= 0.50) return "OK";
      if (v >= 0.35) return "WEAK";
      return "SKIP";
    };

    const confLabel = `${getConfidenceLabel(confidence)} ${getConfidenceBadge(confidence)}`;
    const divLabel = getDivergenceLabel(divergenceScore);

    // Latency formatters
    const formatLatency = (ms: number | undefined) => {
      if (ms === undefined || isNaN(ms)) return "❌ offline";
      if (ms < 100) return `✅ <code>${ms}ms</code>`;
      if (ms < 300) return `⚠️ <code>${ms}ms</code>`;
      return `🔴 <code>${ms}ms</code>`;
    };

    const formatOffset = (ms: number | undefined) => {
      if (ms === undefined || isNaN(ms)) return "❌ unknown";
      const s = ms / 1000;
      const sign = s >= 0 ? "+" : "";
      const absS = Math.abs(s).toFixed(1);
      if (Math.abs(ms) < 1000) return `✅ offset <code>${sign}${absS}s</code>`;
      return `⚠️ offset <code>${sign}${absS}s</code>`;
    };

    // Status Line logic
    let statusStr = "⏳ WAITING";
    if (paused) {
      statusStr = "⏸ PAUSED";
    } else if (hasPosition) {
      statusStr = `📥 HOLDING — exit at ${profitTargetPct.toFixed(0)}% or ${exitBeforeFinalSecs}s`;
    } else if (timeRemainingSecs <= 30 && timeRemainingSecs > 0) {
      statusStr = "🚫 FINAL WINDOW — no trades";
    } else if (tradeable) {
      statusStr = "🟢 TRADEABLE";
    }

    const trendText = trendEmoji(btcTrend);
    const velStr = priceVelocity >= 0 ? `+${priceVelocity.toFixed(2)}` : priceVelocity.toFixed(2);
    const volDeltaStr = volumeDelta >= 0 ? `+${volumeDelta.toLocaleString()}` : volumeDelta.toLocaleString();

    const lines = [
      title,
      "",
      "━━━ MARKET ━━━━━━━━━━━━━━━━━",
      `📍 <code>${marketId}</code>`,
      timeRemainingSecs > 0
        ? `⏱  <code>${timeRemainingSecs}s</code> remaining · ACTIVE`
        : `⏱  —`,
      "",
      `<code>YES:  ${yesPrice.toFixed(3)}  NO:  ${noPrice.toFixed(3)}</code>`,
      "",
      "━━━ EDGE ANALYSIS ━━━━━━━━━━",
      (() => {
        const modeEmoji =
          thresholdMode === "FLOOR" ? "🔴" : thresholdMode === "RELAXED" ? "🟡" : "🟢";
        return `Mode:          ${modeEmoji} <code>${thresholdMode}</code> (<code>${minsSinceLastTrade} min</code> since last trade)`;
      })(),
      `Thresholds:    edge >= <code>${(activeMinEdgePct * 100).toFixed(0)}%</code> · conf >= <code>${activeMinConfidence.toFixed(2)}</code>`,
      `Divergence:    <code>${divergenceScore.toFixed(2)} (${divLabel})</code>`,
      `Est. repricing:<code>${expectedRepricing >= 0 ? "+" : ""}${(expectedRepricing * 100).toFixed(1)}%</code>`,
      `Direction:     <code>${bestDir === "YES" ? "⬆️ YES" : "⬇️ NO"} @ ${bestPrice.toFixed(3)}</code>`,
      `Confidence:    <code>${confidence.toFixed(2)} (${confLabel})</code>`,
      `Status:        <b>${statusStr}</b>`,
    ];

    if (missingReason && missingReason !== "None" && !tradeable && !hasPosition && !paused) {
      lines.push(`Missing:       <code>${missingReason}</code>`);
    }

    lines.push(
      "",
      "━━━ BTC ━━━━━━━━━━━━━━━━━━━━",
      `<b>$${btcPrice.toLocaleString()}</b>  vel: <code>${velStr}/s</code>  trend: ${trendText} ${btcTrend} (${velocityTrend})`,
      `vol delta: <code>${volDeltaStr}</code>`,
      "",
      "━━━ POSITION ━━━━━━━━━━━━━━━",
    );

    if (hasPosition) {
      const p = positions[0];
      const holdTimeSecs = Math.floor((Date.now() - p.entry_at_ms) / 1000);
      lines.push(
        `Direction:  <code>${p.direction === "Yes" ? "⬆️ YES" : "⬇️ NO"} @ ${p.entry_price.toFixed(3)}</code>`,
        `Size:       <code>$${p.size_usd.toFixed(2)}</code> → <code>${p.share_qty.toFixed(2)}</code> shares`,
        `Hold:       <code>${holdTimeSecs}s</code>`
      );
    } else {
      lines.push("None open");
    }

    const getLatencyIcon = (ms: number | undefined) => {
      if (ms === undefined || isNaN(ms) || ms === 0) return "🔴";
      if (ms < 100) return "✅";
      if (ms <= 500) return "🟡";
      return "🔴";
    };

    const formatBinanceFeed = (avgMs: number | undefined, pingMs: number | undefined, ticksPerSec: number) => {
      const isOffline = avgMs === undefined || isNaN(avgMs) || avgMs === 0;
      const latencyIcon = getLatencyIcon(avgMs);
      const avgText = isOffline ? "offline" : `${avgMs}ms avg`;
      const pingText = pingMs !== undefined && pingMs > 0 ? ` (ping: ${pingMs}ms)` : "";
      return `${latencyIcon} ${avgText}${pingText} · ticks: ${ticksPerSec.toFixed(1)}/s`;
    };

    const formatPolymarketFeed = (avgMs: number | undefined, ticksPerSec: number, isFallback: boolean) => {
      const isOffline = avgMs === undefined || isNaN(avgMs) || avgMs === 0;
      const latencyIcon = getLatencyIcon(avgMs);
      const avgText = isOffline ? "offline" : `${avgMs}ms`;
      const fallbackText = isFallback ? " · FALLBACK REST" : "";
      return `${latencyIcon} ${avgText} · ticks: ${ticksPerSec.toFixed(1)}/s${fallbackText}`;
    };

    const formatAgeSecs = (lastMs: number | undefined): { text: string; icon: string } => {
      if (!lastMs || lastMs <= 0) return { text: "unknown", icon: "🔴" };
      const ageSecs = Math.max(0, (Date.now() - lastMs) / 1000);
      const icon = ageSecs < 2 ? "✅" : ageSecs < 8 ? "🟡" : "🔴";
      return { text: `${ageSecs.toFixed(1)}s ago`, icon };
    };

    const formatClockSync = (ms: number | undefined) => {
      if (ms === undefined || isNaN(ms)) return "🔴 offset unknown";
      const s = ms / 1000;
      const sign = s >= 0 ? "+" : "";
      const absS = Math.abs(s).toFixed(1);
      const icon = Math.abs(ms) < 1000 ? "✅" : "🟡";
      return `${icon} offset ${sign}${absS}s`;
    };

    const priceAge = formatAgeSecs(polyLastFetchedMs);
    const priceThresholdSecs = fallbackMode ? 8.0 : 2.0;
    const priceFreshnessStr = `${priceAge.icon} ${priceAge.text} (threshold: ${priceThresholdSecs.toFixed(1)}s)`;

    const validatorIcon = priceRejections === 0 ? "✅" : "🟡";
    const validatorStr = `${validatorIcon} ${priceRejections} rejected this session`;

    const polyMsgAge = formatAgeSecs(polyLastMsgMs);
    const wsStatusIcon = fallbackMode ? "🟡" : "✅";
    const wsStatusText = fallbackMode ? "fallback active" : "live WS";

    lines.push(
      "",
      "━━━ FEEDS ━━━━━━━━━━━━━━━━━━",
      `Binance:     ${formatBinanceFeed(binanceLat, binancePing, binanceTickRate)}`,
      `Polymarket:  ${formatPolymarketFeed(polyLat, polyTickRate, fallbackMode)}`,
      `             ${polyMsgAge.icon} last message: ${polyMsgAge.text}`,
      `Clock sync:  ${formatClockSync(clockOffset)}`,
      "",
      "Data quality:",
      `  Price freshness:  ${priceFreshnessStr}`,
      `  WS status:        ${wsStatusIcon} ${wsStatusText}`,
      `  Rejected updates: ${validatorStr}`
    );

    await ctx.reply(lines.join("\n"), { parse_mode: "HTML" });
  } catch (err) {
    await ctx.reply(`❌ Error: ${(err as Error).message}`);
  }
}
