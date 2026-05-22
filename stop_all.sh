#!/bin/bash
# stop_all.sh — Gracefully stop all PolyTrade services

echo "🛑 Stopping PolyTrade Services..."

# Stop Rust Core Engine
if pkill -f "target/release/polytrade" 2>/dev/null; then
    echo "✅ Rust Trading Engine stopped"
else
    echo "ℹ️  Rust Trading Engine was not running"
fi

# Stop Telegram Bot
if pkill -f "dist/index.js" 2>/dev/null; then
    echo "✅ Telegram Bot stopped"
else
    echo "ℹ️  Telegram Bot was not running"
fi

# Also catch any tsx dev instances
pkill -f "tsx src/index.ts" 2>/dev/null || true

# Wait a moment for clean exit
sleep 1

# Verify nothing is left
REMAINING=$(ps aux | grep -E "target/release/polytrade|dist/index.js" | grep -v grep | wc -l)
if [ "$REMAINING" -gt 0 ]; then
    echo "⚠️  Force killing remaining processes..."
    pkill -9 -f "target/release/polytrade" 2>/dev/null || true
    pkill -9 -f "dist/index.js" 2>/dev/null || true
fi

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ All PolyTrade services stopped"
echo "   Redis still running — stop manually if needed:"
echo "   ~/redis-bin/usr/bin/redis-cli shutdown"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
