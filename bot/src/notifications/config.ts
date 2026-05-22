/** Telegram push notification toggles (defaults match config/default.toml). */
export const telegramNotifications = {
  position_opened: process.env.TELEGRAM_NOTIFY_POSITION_OPENED !== "false",
  position_closed: process.env.TELEGRAM_NOTIFY_POSITION_CLOSED !== "false",
  kill_switch: process.env.TELEGRAM_NOTIFY_KILL_SWITCH !== "false",
  market_transition: process.env.TELEGRAM_NOTIFY_MARKET_TRANSITION === "true",
  critical_feed_error: process.env.TELEGRAM_NOTIFY_CRITICAL_FEED !== "false",
  edge_monitor: false,
  signal_detected: false,
  health_periodic: false,
  risk_alert: false,
};
