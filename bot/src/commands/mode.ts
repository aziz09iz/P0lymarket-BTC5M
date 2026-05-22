import type { Context } from "grammy";
import type { Redis } from "ioredis";

import { getConfig, setConfig, auditLog } from "../redis/client.js";

// Track pending confirmations: userId → expiry timestamp.
const pendingLiveConfirm = new Map<number, number>();

export async function handleMode(ctx: Context, redis: Redis): Promise<void> {
  try {
    const text = ctx.message?.text ?? "";
    const parts = text.trim().split(/\s+/);
    const currentMode =
      (await getConfig(redis, "polytrade:config:mode")) ?? "paper";
    const userId = ctx.from?.id ?? 0;

    // /mode — show current mode.
    if (parts.length === 1) {
      const emoji = currentMode === "live" ? "🔴 LIVE" : "🟡 PAPER";
      await ctx.reply(`Current mode: ${emoji}`);
      return;
    }

    const target = parts[1]?.toLowerCase();

    // /mode paper — switch to paper.
    if (target === "paper") {
      await setConfig(redis, "polytrade:config:mode", "paper");
      await auditLog(redis, "mode_change", `switched to paper by user ${userId}`);
      await ctx.reply("🟡 Switched to <b>PAPER MODE</b>\nNo real orders will be placed.", {
        parse_mode: "HTML",
      });
      return;
    }

    // /mode live — prompt confirmation.
    if (target === "live" && parts[2] !== "confirm") {
      pendingLiveConfirm.set(userId, Date.now() + 30_000);
      await ctx.reply(
        [
          `⚠️ <b>Switch to LIVE mode?</b>`,
          `This will enable REAL order placement on Polymarket.`,
          `Make sure you have reviewed all risk settings.`,
          ``,
          `Confirm with: <code>/mode live confirm</code>`,
          `(expires in 30 seconds)`,
        ].join("\n"),
        { parse_mode: "HTML" }
      );
      return;
    }

    // /mode live confirm — actually switch.
    if (target === "live" && parts[2] === "confirm") {
      const expiry = pendingLiveConfirm.get(userId);
      if (!expiry || Date.now() > expiry) {
        await ctx.reply("⏰ Confirmation expired. Run <code>/mode live</code> again.", {
          parse_mode: "HTML",
        });
        pendingLiveConfirm.delete(userId);
        return;
      }
      pendingLiveConfirm.delete(userId);

      const sizeUsd =
        (await getConfig(redis, "polytrade:config:size_usd")) ?? "1.00";

      await setConfig(redis, "polytrade:config:mode", "live");
      await auditLog(redis, "mode_change", `switched to LIVE by user ${userId}`);

      await ctx.reply(
        [
          `🔴 <b>LIVE MODE ACTIVATED</b>`,
          `All trades will now use real funds.`,
          `Current size: $${Number(sizeUsd).toFixed(2)} per trade`,
          `Use /pause to halt immediately.`,
        ].join("\n"),
        { parse_mode: "HTML" }
      );
      return;
    }

    await ctx.reply("Usage: /mode [paper | live | live confirm]");
  } catch (err) {
    await ctx.reply(`❌ Error: ${(err as Error).message}`);
  }
}
