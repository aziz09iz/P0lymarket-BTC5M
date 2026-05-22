import type { Context } from "grammy";
import type { Redis } from "ioredis";
import { handleStatus } from "./commands/status.js";
import { handlePnl } from "./commands/pnl.js";
import { handleMode } from "./commands/mode.js";
import { handleSetSize } from "./commands/size.js";
import { handleRisk } from "./commands/risk.js";
import { handlePause } from "./commands/pause.js";
import { handleResume } from "./commands/resume.js";

// Mocking Context and Redis
class MockContext {
  replies: { text: string; options?: any }[] = [];
  message = { text: "" };
  from = { id: 123456789 };

  reply(text: string, options?: any) {
    this.replies.push({ text, options });
    return Promise.resolve({} as any);
  }
}

class MockRedis {
  store: Record<string, string> = {};
  lists: Record<string, string[]> = {};
  hashes: Record<string, Record<string, string>> = {};

  async get(key: string): Promise<string | null> {
    return this.store[key] ?? null;
  }

  async set(key: string, value: string): Promise<void> {
    this.store[key] = value;
  }

  async keys(pattern: string): Promise<string[]> {
    const regex = new RegExp("^" + pattern.replace(/\*/g, ".*") + "$");
    return Object.keys(this.store).filter((k) => regex.test(k));
  }

  async hget(key: string, field: string): Promise<string | null> {
    return this.hashes[key]?.[field] ?? null;
  }

  async hset(key: string, field: string, value: string): Promise<number> {
    if (!this.hashes[key]) this.hashes[key] = {};
    this.hashes[key]![field] = value;
    return 1;
  }

  async hgetall(key: string): Promise<Record<string, string>> {
    return this.hashes[key] ?? {};
  }

  async lrange(key: string, start: number, stop: number): Promise<string[]> {
    const list = this.lists[key] ?? [];
    if (stop === -1) return list.slice(start);
    return list.slice(start, stop + 1);
  }

  async lpush(key: string, ...values: string[]): Promise<number> {
    if (!this.lists[key]) this.lists[key] = [];
    this.lists[key]!.unshift(...values);
    return this.lists[key]!.length;
  }

  async ltrim(key: string, start: number, stop: number): Promise<string> {
    if (this.lists[key]) {
      this.lists[key] = this.lists[key]!.slice(start, stop + 1);
    }
    return "OK";
  }

  async expire(key: string, seconds: number): Promise<number> {
    return 1;
  }
}

async function runTests() {
  console.log("🧪 Starting Command Handlers Unit Tests...");

  const redis = new MockRedis() as unknown as Redis;

  // Set up mock data
  await redis.set(
    "polytrade:state:btc",
    JSON.stringify({
      price: 67420,
      price_velocity: 0.12,
      volume_delta: 5,
      microtrend: "Bullish",
    })
  );
  await redis.set("polytrade:config:mode", "paper");
  await redis.set("polytrade:config:paused", "false");
  
  // Set up polytrade:signal:latest with all metadata
  await redis.set(
    "polytrade:signal:latest",
    JSON.stringify({
      market_id: "0x123456789abcdef",
      time_remaining_secs: 240,
      poly_yes_pct: 0.52,
      poly_no_pct: 0.48,
      divergence_score: 0.45,
      expected_repricing: 0.05,
      edge_pct: 0.04,
      tradeable: false,
      direction: "YES",
      confidence: 0.48,
      btc_price: 67420,
      price_velocity: 0.12,
      btc_trend: "Bullish",
      velocity_trend: "Stable",
      volume_delta: 5.0,
      binance_latency_ms: 12,
      polymarket_latency_ms: 34,
      clock_offset_ms: 200,
      binance_ping_ms: 10,
      binance_tick_rate: 3.5,
      polymarket_tick_rate: 0.3,
      binance_last_msg_ms: Date.now() - 5000,
      polymarket_last_msg_ms: Date.now() - 8000,
      price_rejections: 2,
      fallback_mode: false,
      poly_last_fetched_ms: Date.now() - 1000,
      missing_reason: "None"
    })
  );

  // Test /status
  {
    const ctx = new MockContext() as unknown as Context;
    await handleStatus(ctx, redis);
    const reply = ctx.replies[0]?.text ?? "";
    console.assert(reply.includes("PAPER"), "Status should show PAPER mode");
    console.assert(reply.includes("67,420"), "Status should show BTC price");
    console.assert(reply.includes("12ms avg"), "Status should show Binance latency");
    console.assert(reply.includes("34ms"), "Status should show Polymarket latency");
    console.assert(reply.includes("2 rejected this session"), "Status should show validator price rejections");
    console.log("✅ /status command test passed");
  }

  // Test /pnl
  {
    // Populate hash stats
    await redis.hset("polytrade:paper:stats", "trade_count", "2");
    await redis.hset("polytrade:paper:stats", "total_pnl", "0.10");
    await redis.hset("polytrade:paper:stats", "win_count", "1");
    await redis.hset("polytrade:paper:stats", "loss_count", "1");
    await redis.hset("polytrade:paper:stats", "avg_win", "0.20");
    await redis.hset("polytrade:paper:stats", "avg_loss", "-0.10");
    await redis.hset("polytrade:paper:stats", "best_trade", "0.20");
    await redis.hset("polytrade:paper:stats", "worst_trade", "-0.10");
    await redis.hset("polytrade:paper:stats", "edge_avg", "0.05");
    await redis.hset("polytrade:paper:stats", "avg_hold_secs", "7.5");
    await redis.hset("polytrade:paper:stats", "yes_trades", "1");
    await redis.hset("polytrade:paper:stats", "yes_wins", "1");
    await redis.hset("polytrade:paper:stats", "no_trades", "1");
    await redis.hset("polytrade:paper:stats", "no_wins", "0");
    await redis.hset("polytrade:paper:stats", "suspicious_count", "1");

    // Populate a suspicious trade
    const suspTrade = {
      market_id: "m_susp",
      direction: "NO",
      entry_price: 0.070,
      exit_price: 0.500,
      pnl_pct: 614.0,
      hold_duration_ms: 17000,
      suspicious_reason: "jump: 0.430, allowed: 0.003",
    };
    await redis.lpush("polytrade:paper:suspicious_trades", JSON.stringify(suspTrade));

    const ctx = new MockContext() as unknown as Context;
    await handlePnl(ctx, redis);
    const reply = ctx.replies[0]?.text ?? "";

    console.assert(reply.includes("Real PnL:       <code>+$0.10</code>"), "PnL should match aggregated PnL");
    console.assert(reply.includes("Valid trades:      <code>2</code>"), "PnL should show correct trade count");
    console.assert(reply.includes("WR: <code>50%</code>"), "PnL should show correct win rate");
    console.assert(reply.includes("Best trade:     <code>+$0.20</code>"), "PnL should show correct best trade");
    console.assert(reply.includes("Worst trade:    <code>-$0.10</code>"), "PnL should show correct worst trade");
    console.assert(reply.includes("Suspicious trades: <code>1</code>"), "PnL should show correct suspicious trades count");
    console.assert(reply.includes("jump: 0.430, allowed: 0.003"), "PnL should list the suspicious trade details");
    console.log("✅ /pnl command test passed");
  }

  // Test /mode paper
  {
    const ctx = new MockContext() as unknown as Context;
    ctx.message.text = "/mode paper";
    await handleMode(ctx, redis);
    const reply = ctx.replies[0]?.text ?? "";
    console.assert(
      reply.includes("Switched to") && reply.includes("PAPER MODE"),
      "Should confirm paper mode switch"
    );
    console.log("✅ /mode paper test passed");
  }

  // Test /mode live (requires confirmation)
  {
    const ctx = new MockContext() as unknown as Context;
    ctx.message.text = "/mode live";
    await handleMode(ctx, redis);
    console.assert(
      ctx.replies[0]?.text.includes("Switch to LIVE mode?"),
      "Should ask for confirmation"
    );

    // Confirm transition
    ctx.message.text = "/mode live confirm";
    await handleMode(ctx, redis);
    console.assert(
      ctx.replies[1]?.text.includes("LIVE MODE ACTIVATED"),
      "Should activate live mode on confirmation"
    );
    console.log("✅ /mode live confirm flow test passed");
  }

  // Test /setsize
  {
    const ctx = new MockContext() as unknown as Context;
    await redis.set("polytrade:config:max_exposure_usd", "10");
    await redis.set("polytrade:config:max_concurrent_positions", "3");

    // Invalid size below min
    ctx.message.text = "/setsize 0.05";
    await handleSetSize(ctx, redis);
    console.assert(
      ctx.replies[0]?.text.includes("Size must be between $0.10 and $100.00"),
      "Should reject size below $0.10"
    );

    // Size exceeding exposure limit
    ctx.message.text = "/setsize 5.00";
    await handleSetSize(ctx, redis);
    console.assert(
      ctx.replies[1]?.text.includes("Exceeds max exposure limit"),
      "Should reject size exceeding exposure limit"
    );

    // Valid size
    ctx.message.text = "/setsize 2.50";
    await handleSetSize(ctx, redis);
    const reply = ctx.replies[2]?.text ?? "";
    console.assert(
      reply.includes("Order size set to") && reply.includes("2.50"),
      "Should successfully set valid size"
    );
    const storedSize = await redis.get("polytrade:config:size_usd");
    console.assert(storedSize === "2.50", "Stored size should be updated in Redis");
    console.log("✅ /setsize command validation test passed");
  }

  // Test /risk
  {
    const ctx = new MockContext() as unknown as Context;
    await handleRisk(ctx, redis);
    const reply = ctx.replies[0]?.text ?? "";
    console.assert(reply.includes("<code>3</code>"), "Risk should show max positions");
    console.assert(reply.includes("<code>$2.50</code>"), "Risk should show updated size");
    console.log("✅ /risk command test passed");
  }

  // Test /pause and /resume
  {
    const ctx = new MockContext() as unknown as Context;
    await handlePause(ctx, redis);
    console.assert(
      ctx.replies[0]?.text.includes("Bot paused"),
      "Pause command should confirm pausing"
    );
    let paused = await redis.get("polytrade:config:paused");
    console.assert(paused === "true", "Redis paused config should be true");

    await handleResume(ctx, redis);
    console.assert(
      ctx.replies[1]?.text.includes("Bot resumed"),
      "Resume command should confirm resuming"
    );
    paused = await redis.get("polytrade:config:paused");
    console.assert(paused === "false", "Redis paused config should be false");
    console.log("✅ /pause and /resume commands test passed");
  }

  console.log("🎉 All command tests passed successfully!");
}

runTests().catch((err) => {
  console.error("❌ Test failed with error:", err);
  process.exit(1);
});
