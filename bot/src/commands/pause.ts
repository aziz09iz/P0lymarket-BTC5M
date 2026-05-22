import type { Context } from "grammy";
import type { Redis } from "ioredis";

import { setConfig, auditLog } from "../redis/client.js";

export async function handlePause(ctx: Context, redis: Redis): Promise<void> {
  try {
    const userId = ctx.from?.id ?? 0;
    await setConfig(redis, "polytrade:config:paused", "true");
    await auditLog(redis, "pause", `paused by user ${userId}`);
    await ctx.reply(
      [
        `⏸ <b>Bot paused.</b>`,
        `No new signals will be traded.`,
        `Open positions remain open.`,
        `Use /resume to restart.`,
      ].join("\n"),
      { parse_mode: "HTML" }
    );
  } catch (err) {
    await ctx.reply(`❌ Error: ${(err as Error).message}`);
  }
}
