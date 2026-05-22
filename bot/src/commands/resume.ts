import type { Context } from "grammy";
import type { Redis } from "ioredis";

import { setConfig, auditLog } from "../redis/client.js";

export async function handleResume(ctx: Context, redis: Redis): Promise<void> {
  try {
    const userId = ctx.from?.id ?? 0;
    await setConfig(redis, "polytrade:config:paused", "false");
    await auditLog(redis, "resume", `resumed by user ${userId}`);
    await ctx.reply("▶️ <b>Bot resumed.</b> Signal evaluation active.", {
      parse_mode: "HTML",
    });
  } catch (err) {
    await ctx.reply(`❌ Error: ${(err as Error).message}`);
  }
}
