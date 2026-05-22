#!/bin/bash
# start_all.sh — Launch Redis, Rust Engine, and Telegram Bot

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🚀 Starting PolyTrade Services..."

# 1. Start Redis Server if not running
export LD_LIBRARY_PATH="$HOME/redis-bin/usr/lib/x86_64-linux-gnu:$LD_LIBRARY_PATH"
REDIS_CLI_CMD="$HOME/redis-bin/usr/bin/redis-cli"

if "$REDIS_CLI_CMD" ping &>/dev/null; then
    echo "✅ Redis is already running on port 6379"
else
    echo "🔑 Starting local Redis server..."
    
    # Create redis dump directory if needed
    mkdir -p "$SCRIPT_DIR/data"
    
    # Start redis-server daemonized
    "$HOME/redis-bin/usr/bin/redis-server" --port 6379 --dir "$SCRIPT_DIR/data" --daemonize yes
    
    # Wait for Redis to start
    for i in {1..5}; do
        if "$REDIS_CLI_CMD" ping 2>/dev/null | grep -q "PONG"; then
            echo "✅ Redis started successfully"
            break
        fi
        sleep 1
    done
    
    if ! "$REDIS_CLI_CMD" ping &>/dev/null; then
        echo "❌ Failed to start Redis server. Exiting."
        exit 1
    fi
fi

# 2. Start Rust Core Engine
echo "⚙️ Starting Rust Trading Engine in the background..."
export PATH="$HOME/.cargo/bin:$HOME/gcc-extract/extracted/usr/bin:$PATH"
export CC="$HOME/.cargo/bin/cc-wrapper"

# Kill any existing rust process
pkill -f "target/release/polytrade" 2>/dev/null || true
pkill -f "target/debug/polytrade" 2>/dev/null || true

setsid ./target/release/polytrade < /dev/null >> "$SCRIPT_DIR/engine.log" 2>&1 & disown
ENGINE_PID=$!
echo "✅ Rust Trading Engine started (PID: $ENGINE_PID, logging to engine.log)"

# 3. Start Telegram Bot
echo "🤖 Starting Telegram Bot in the background..."
export PATH="$HOME/node/bin:$PATH"

# Kill any existing bot process
pkill -f "tsx src/index.ts" 2>/dev/null || true
pkill -f "dist/index.js" 2>/dev/null || true

cd "$SCRIPT_DIR/bot"
setsid node dist/index.js < /dev/null >> "$SCRIPT_DIR/bot.log" 2>&1 & disown
BOT_PID=$!
echo "✅ Telegram Bot started (PID: $BOT_PID, logging to bot.log)"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🎉 All services started successfully!"
echo "• Monitor Rust Core logs: tail -f engine.log"
echo "• Monitor Telegram Bot logs: tail -f bot.log"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
