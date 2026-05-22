import { Redis } from "ioredis";

// ---------------------------------------------------------------------------
// Redis client singleton
// ---------------------------------------------------------------------------

let commandClient: Redis | null = null;
let subClient: Redis | null = null;

const RETRY_DELAY_MS = 5_000;

function createClient(url: string, name: string): Redis {
  const client = new Redis(url, {
    retryStrategy(times: number) {
      console.log(`[redis:${name}] reconnecting (attempt ${times})…`);
      return Math.min(times * RETRY_DELAY_MS, 30_000);
    },
    maxRetriesPerRequest: null,
    lazyConnect: true,
  });

  client.on("connect", () => console.log(`[redis:${name}] connected`));
  client.on("error", (err: Error) =>
    console.error(`[redis:${name}] error: ${err.message}`)
  );

  return client;
}

/** Get the shared command client (for GET/SET/HSET etc). */
export function getCommandClient(url: string): Redis {
  if (!commandClient) {
    commandClient = createClient(url, "cmd");
  }
  return commandClient;
}

/** Get a dedicated subscriber client (for SUBSCRIBE). */
export function getSubscriberClient(url: string): Redis {
  if (!subClient) {
    subClient = createClient(url, "sub");
  }
  return subClient;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Read a config key with fallback. */
export async function getConfig(
  redis: Redis,
  key: string,
  fallback?: string
): Promise<string | null> {
  const val = await redis.get(key);
  return val ?? fallback ?? null;
}

/** Write a config key. */
export async function setConfig(
  redis: Redis,
  key: string,
  value: string
): Promise<void> {
  await redis.set(key, value);
}

/** Append to the audit log with a 7-day TTL. */
export async function auditLog(
  redis: Redis,
  action: string,
  details: string
): Promise<void> {
  const entry = JSON.stringify({
    action,
    details,
    timestamp: new Date().toISOString(),
  });
  await redis.lpush("polytrade:audit:log", entry);
  await redis.ltrim("polytrade:audit:log", 0, 999);
  await redis.expire("polytrade:audit:log", 7 * 24 * 3600);
}

/** Gracefully disconnect all clients. */
export async function disconnectAll(): Promise<void> {
  if (commandClient) await commandClient.quit().catch(() => {});
  if (subClient) await subClient.quit().catch(() => {});
}
