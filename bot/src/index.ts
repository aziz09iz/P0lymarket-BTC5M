import "dotenv/config";
import { Bot } from "grammy";
import {
  getCommandClient,
  getSubscriberClient,
  disconnectAll,
} from "./redis/client.js";
import { startNotificationPublisher } from "./notifications/publisher.js";

// Primary commands
import { handleStatus } from "./commands/status.js";
import { handlePnl } from "./commands/pnl.js";
import { handleMode } from "./commands/mode.js";
import { handleSetSize } from "./commands/size.js";
import { handleRisk } from "./commands/risk.js";
import { handlePause } from "./commands/pause.js";
import { handleResume } from "./commands/resume.js";

// ---------------------------------------------------------------------------
// Environment
// ---------------------------------------------------------------------------

const BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
if (!BOT_TOKEN) {
  console.error("TELEGRAM_BOT_TOKEN is required. Set it in bot/.env");
  process.exit(1);
}

const ALLOWED_IDS: Set<number> = new Set(
  (process.env.ALLOWED_USER_IDS ?? "")
    .split(",")
    .map((s) => parseInt(s.trim(), 10))
    .filter((n) => !isNaN(n))
);

const REDIS_URL = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";

// ---------------------------------------------------------------------------
// Bot setup
// ---------------------------------------------------------------------------

const bot = new Bot(BOT_TOKEN);
const redis = getCommandClient(REDIS_URL);

// Auth middleware — silently ignore non-whitelisted users.
bot.use(async (ctx, next) => {
  const userId = ctx.from?.id;
  if (!userId || (ALLOWED_IDS.size > 0 && !ALLOWED_IDS.has(userId))) {
    return; // Silently ignore.
  }
  await next();
});

// ---------------------------------------------------------------------------
// /start — command menu
// ---------------------------------------------------------------------------

bot.command("start", (ctx) =>
  ctx.reply(
    [
      `🤖 <b>PolyTrade 5M Bot</b>`,
      ``,
      `/start    — Show this menu`,
      `/status   — Full dashboard (market + edge + position + feeds)`,
      `/pnl      — PnL + performance summary`,
      `/setsize  — Set trade size in dollars`,
      `/mode     — View or switch paper/live mode`,
      `/risk     — View and update risk settings`,
      `/pause    — Pause all trading`,
      `/resume   — Resume trading`,
    ].join("\n"),
    { parse_mode: "HTML" }
  )
);

// ---------------------------------------------------------------------------
// Primary command handlers
// ---------------------------------------------------------------------------

bot.command("status", (ctx) => handleStatus(ctx, redis));
bot.command("pnl", (ctx) => handlePnl(ctx, redis));
bot.command("mode", (ctx) => handleMode(ctx, redis));
bot.command("setsize", (ctx) => handleSetSize(ctx, redis));
bot.command("risk", (ctx) => handleRisk(ctx, redis));
bot.command("pause", (ctx) => handlePause(ctx, redis));
bot.command("resume", (ctx) => handleResume(ctx, redis));

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  console.log("[bot] connecting to Redis…");
  redis.connect().catch((err: Error) => {
    console.warn(`[bot] Redis initial connection failed (${err.message}). Reconnecting in background…`);
  });

  // Start real-time notifications (live mode event relay).
  const chatIds = Array.from(ALLOWED_IDS);
  if (chatIds.length > 0) {
    const subRedis = getSubscriberClient(REDIS_URL);
    startNotificationPublisher(bot, subRedis, redis, chatIds).catch((err: Error) =>
      console.error("[notifications] failed to start:", err.message)
    );
  }

  console.log("[bot] starting Telegram polling…");
  await bot.start({
    onStart: () => console.log("[bot] ✅ PolyTrade Pro Bot is running"),
  });
}

// Graceful shutdown.
async function shutdown(): Promise<void> {
  console.log("[bot] shutting down…");
  bot.stop();
  await disconnectAll();
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

main().catch((err) => {
  console.error("[bot] fatal error:", err);
  process.exit(1);
});
