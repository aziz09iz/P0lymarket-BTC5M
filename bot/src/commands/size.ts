import type { Context } from "grammy";
import type { Redis } from "ioredis";

import { getConfig, setConfig, auditLog } from "../redis/client.js";

export async function handleSetSize(
  ctx: Context,
  redis: Redis
): Promise<void> {
  try {
    const text = ctx.message?.text ?? "";
    const parts = text.trim().split(/\s+/);
    const userId = ctx.from?.id ?? 0;

    if (parts.length < 2) {
      const current =
        (await getConfig(redis, "polytrade:config:size_usd")) ?? "1.00";
      await ctx.reply(`Current size: $${Number(current).toFixed(2)}\nUsage: /setsize <amount>`);
      return;
    }

    const amount = parseFloat(parts[1]!);

    if (isNaN(amount) || amount < 0.1 || amount > 100) {
      await ctx.reply("⚠️ Size must be between $0.10 and $100.00");
      return;
    }

    // Cross-check with max exposure.
    const maxExposureRaw =
      (await getConfig(redis, "polytrade:config:max_exposure_usd")) ?? "10";
    const maxPositionsRaw =
      (await getConfig(redis, "polytrade:config:max_concurrent_positions")) ??
      "3";
    const maxExposure = parseFloat(maxExposureRaw);
    const maxPositions = parseInt(maxPositionsRaw, 10);

    const totalExposure = amount * maxPositions;

    if (totalExposure > maxExposure) {
      await ctx.reply(
        [
          `⚠️ $${amount.toFixed(2)} × ${maxPositions} max positions = $${totalExposure.toFixed(2)}`,
          `Exceeds max exposure limit of $${maxExposure.toFixed(2)}`,
          `Reduce max positions or increase limit.`,
          `Use /risk to view current settings.`,
        ].join("\n")
      );
      return;
    }

    await setConfig(redis, "polytrade:config:size_usd", amount.toFixed(2));
    await auditLog(
      redis,
      "size_change",
      `set size to $${amount.toFixed(2)} by user ${userId}`
    );

    await ctx.reply(
      [
        `✅ Order size set to <b>$${amount.toFixed(2)}</b> per trade`,
        `Max exposure: ${maxPositions} positions × $${amount.toFixed(2)} = $${totalExposure.toFixed(2)}`,
        `(within risk limit of $${maxExposure.toFixed(2)})`,
      ].join("\n"),
      { parse_mode: "HTML" }
    );
  } catch (err) {
    await ctx.reply(`❌ Error: ${(err as Error).message}`);
  }
}
