import type { Context } from "grammy";
import type { Redis } from "ioredis";
import { getConfig, setConfig, auditLog } from "../redis/client.js";

export async function handleRisk(ctx: Context, redis: Redis): Promise<void> {
  try {
    const text = ctx.message?.text ?? "";
    const parts = text.trim().split(/\s+/);
    const userId = ctx.from?.id ?? 0;

    // Setting a risk setting
    if (parts.length >= 3) {
      const param = parts[1]!.toLowerCase();
      const rawValue = parts[2]!;
      let key = "";
      let configName = "";
      let displayValue = "";
      let parsedValue: number;

      if (param === "maxloss") {
        parsedValue = parseInt(rawValue, 10);
        if (isNaN(parsedValue) || parsedValue < 1 || parsedValue > 20) {
          await ctx.reply("⚠️ consecutive_loss_limit must be an integer between 1 and 20");
          return;
        }
        key = "polytrade:config:consecutive_loss_limit";
        configName = "consecutive_loss_limit";
        displayValue = parsedValue.toString();
      } else if (param === "cooldown") {
        parsedValue = parseInt(rawValue, 10);
        if (isNaN(parsedValue) || parsedValue < 1 || parsedValue > 3600) {
          await ctx.reply("⚠️ cooldown_after_loss_secs must be an integer between 1 and 3600");
          return;
        }
        key = "polytrade:config:cooldown_after_loss_secs";
        configName = "cooldown_after_loss_secs";
        displayValue = `${parsedValue}s`;
      } else if (param === "minedge") {
        parsedValue = parseFloat(rawValue);
        if (isNaN(parsedValue) || parsedValue < 0 || parsedValue > 100) {
          await ctx.reply("⚠️ min_edge_pct must be a number between 0% and 100%");
          return;
        }
        key = "polytrade:config:min_edge_pct";
        configName = "min_edge_pct";
        displayValue = `${parsedValue.toFixed(1)}%`;
      } else if (param === "maxspread") {
        parsedValue = parseFloat(rawValue);
        if (isNaN(parsedValue) || parsedValue < 0 || parsedValue > 100) {
          await ctx.reply("⚠️ max_spread_for_trade must be a number between 0% and 100%");
          return;
        }
        key = "polytrade:config:max_spread_for_trade";
        configName = "max_spread_for_trade";
        displayValue = `${parsedValue.toFixed(1)}%`;
      } else {
        await ctx.reply("⚠️ Unknown parameter. Available: maxloss, cooldown, minedge, maxspread");
        return;
      }

      // If minedge or maxspread, store it as float percentage (e.g. "10.00")
      const valToSave = (param === "minedge" || param === "maxspread")
        ? parsedValue.toFixed(2)
        : parsedValue.toString();

      await setConfig(redis, key, valToSave);
      await auditLog(
        redis,
        "risk_change",
        `set risk config ${configName} to ${valToSave} by user ${userId}`
      );

      await ctx.reply(
        [
          `✅ Updated: <code>${configName} = ${displayValue}</code>`,
          `Saved to config. Active immediately.`,
        ].join("\n"),
        { parse_mode: "HTML" }
      );
      return;
    }

    if (parts.length === 2) {
      await ctx.reply(
        [
          `⚠️ Invalid syntax. Usage:`,
          `/risk maxloss <n>   — Set consecutive loss limit`,
          `/risk cooldown <n>  — Set cooldown in seconds`,
          `/risk minedge <n>   — Set min edge %`,
          `/risk maxspread <n> — Set max spread %`,
        ].join("\n")
      );
      return;
    }

    // Viewing settings (no arguments)
    const [
      sizeUsd,
      maxPositions,
      maxExposure,
      consecLimit,
      cooldownSecs,
      exitBeforeFinal,
      minEdge,
      minConf,
      maxSpread,
    ] = await Promise.all([
      getConfig(redis, "polytrade:config:size_usd").then((v) => v ?? "1.00"),
      getConfig(redis, "polytrade:config:max_concurrent_positions").then((v) => v ?? "1"),
      getConfig(redis, "polytrade:config:max_exposure_usd").then((v) => v ?? "10.00"),
      getConfig(redis, "polytrade:config:consecutive_loss_limit").then((v) => v ?? "5"),
      getConfig(redis, "polytrade:config:cooldown_after_loss_secs").then((v) => v ?? "120"),
      getConfig(redis, "polytrade:config:exit_before_final_secs").then((v) => v ?? "30"),
      getConfig(redis, "polytrade:config:min_edge_pct").then((v) => v ?? "8.00"),
      getConfig(redis, "polytrade:config:min_confidence").then((v) => v ?? "0.45"),
      getConfig(redis, "polytrade:config:max_spread_for_trade").then((v) => v ?? "6.00"),
    ]);

    const lines = [
      `⚙️ Risk Settings`,
      `━━━━━━━━━━━━━━━━━━`,
      `Size per trade:     <code>$${Number(sizeUsd).toFixed(2)}</code>   → /setsize <amount>`,
      `Max positions:      <code>${maxPositions}</code> (locked — 1 market 1 position)`,
      `Max exposure:       <code>$${Number(maxExposure).toFixed(2)}</code>`,
      `Loss limit:         <code>${consecLimit}</code> consecutive`,
      `Cooldown:           <code>${cooldownSecs}s</code> after loss limit`,
      `Exit before final:  <code>${exitBeforeFinal}s</code>`,
      `Min edge:           <code>${Number(minEdge).toFixed(1)}%</code>`,
      `Min confidence:     <code>${Number(minConf).toFixed(2)}</code>`,
      `Max spread:         <code>${Number(maxSpread).toFixed(1)}%</code>`,
    ];

    await ctx.reply(lines.join("\n"), { parse_mode: "HTML" });
  } catch (err) {
    await ctx.reply(`❌ Error: ${(err as Error).message}`);
  }
}
