// ---------------------------------------------------------------------------
// Notification message formatters
// ---------------------------------------------------------------------------

interface PaperPosition {
  market_id: string;
  direction: string;
  entry_price: number;
  size_usd: number;
  share_qty: number;
  entry_at_ms: number;
  strategy_name: string;
}

interface ClosedTrade {
  market_id: string;
  direction: string;
  entry_price: number;
  exit_price: number;
  size_usd: number;
  pnl_usd: number;
  pnl_pct: number;
  hold_duration_ms: number;
  exit_reason: string;
  strategy_name: string;
}

/** Short market ID for display. */
function shortId(id: string): string {
  if (id.startsWith("0x") && id.length > 12) {
    return id.slice(0, 8) + "…" + id.slice(-4);
  }
  return id;
}

/** Format a number as USD. */
function usd(n: number): string {
  return `$${n.toFixed(2)}`;
}

/** Format PnL with sign and emoji. */
function pnl(n: number): string {
  const emoji = n >= 0 ? "🟢" : "🔴";
  return `${emoji} ${n >= 0 ? "+" : ""}${usd(n)}`;
}

/** Format exit reason with emoji. */
function exitEmoji(reason: string): string {
  switch (reason) {
    case "ProfitTarget":
      return "✅ Profit target";
    case "StopLoss":
      return "🛑 Stop loss";
    case "TimeExit":
      return "⏰ Time exit";
    case "EdgeGone":
      return "📉 Edge gone";
    default:
      return reason;
  }
}

// ---------------------------------------------------------------------------
// Public formatters
// ---------------------------------------------------------------------------

export function formatPaperOpen(pos: PaperPosition): string {
  return [
    `📥 <b>[PAPER] Position Opened</b>`,
    `Market: <code>${shortId(pos.market_id)}</code>`,
    `Direction: ${pos.direction} @ ${pos.entry_price.toFixed(2)}`,
    `Size: ${usd(pos.size_usd)} | Shares: ${pos.share_qty.toFixed(2)}`,
    `Strategy: ${pos.strategy_name}`,
  ].join("\n");
}

export function formatPaperClose(trade: ClosedTrade): string {
  const holdSecs = (trade.hold_duration_ms / 1000).toFixed(0);
  return [
    `📤 <b>[PAPER] Position Closed</b>`,
    `Market: <code>${shortId(trade.market_id)}</code>`,
    `Exit: ${trade.exit_price.toFixed(2)} | PnL: ${pnl(trade.pnl_usd)} (${trade.pnl_pct >= 0 ? "+" : ""}${trade.pnl_pct.toFixed(1)}%)`,
    `Reason: ${exitEmoji(trade.exit_reason)}`,
    `Hold: ${holdSecs}s`,
  ].join("\n");
}

export function formatRiskAlert(data: {
  consecutive_losses: number;
  cooldown_secs: number;
}): string {
  const resumeAt = new Date(Date.now() + data.cooldown_secs * 1000)
    .toISOString()
    .slice(11, 19);
  return [
    `⚠️ <b>Risk Alert</b>`,
    `${data.consecutive_losses} consecutive losses detected.`,
    `Trading paused for ${data.cooldown_secs} seconds.`,
    `Resume at: ${resumeAt}`,
  ].join("\n");
}
