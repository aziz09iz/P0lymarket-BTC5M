use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::Instant;
use anyhow::{Result, Context};
use tracing::{info, warn};

pub struct PolymarketClock {
    pub server_time_offset_secs: f64,  // server_time - local_time
    last_synced_at: Option<Instant>,
    sync_interval_secs: u64,       // default: 60
}

impl PolymarketClock {
    pub fn new(sync_interval_secs: u64) -> Self {
        Self {
            server_time_offset_secs: 0.0,
            last_synced_at: None,
            sync_interval_secs,
        }
    }

    /// Returns current server time as Unix timestamp (seconds)
    pub fn now(&self) -> f64 {
        let local_now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        local_now + self.server_time_offset_secs
    }

    /// Sync from: GET https://clob.polymarket.com/time
    pub async fn sync(&mut self, client: &reqwest::Client, clob_api: &str) -> Result<()> {
        let url = format!("{}/time", clob_api.trim_end_matches('/'));
        
        let response = client.get(&url)
            .send()
            .await
            .context("Failed to send clock sync request")?;
            
        let text = response.text().await.context("Failed to get time response text")?;
        
        // Handle both: raw f64 string OR JSON { "time": ... }
        let server_time = if let Ok(val) = serde_json::from_str::<serde_json::Value>(&text) {
            if let Some(t) = val.get("time").and_then(|v| v.as_f64()) {
                t
            } else if let Some(t_str) = val.get("time").and_then(|v| v.as_str()) {
                t_str.parse::<f64>().context("Failed to parse time string in JSON")?
            } else {
                text.trim().parse::<f64>().context("Failed to parse fallback response as f64")?
            }
        } else {
            text.trim().parse::<f64>().context("Failed to parse raw time response as f64")?
        };

        let local_now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();

        let new_offset = server_time - local_now;
        
        // Drift warning check: if absolute difference between new offset and old offset is > 5.0s
        if self.last_synced_at.is_some() {
            let drift = (new_offset - self.server_time_offset_secs).abs();
            if drift > 5.0 {
                warn!(
                    drift_secs = %drift,
                    old_offset = %self.server_time_offset_secs,
                    new_offset = %new_offset,
                    "[clock] WARN drift detected: offset={:.4}s, forcing resync",
                    new_offset
                );
            }
        }

        self.server_time_offset_secs = new_offset;
        self.last_synced_at = Some(Instant::now());
        
        info!(
            server_time = %server_time,
            offset = format_args!("{:+.4}s", new_offset),
            "[clock] synced to Polymarket server: offset={:+.4}s",
            new_offset
        );

        Ok(())
    }

    pub fn needs_sync(&self) -> bool {
        match self.last_synced_at {
            None => true,
            Some(last) => last.elapsed().as_secs() >= self.sync_interval_secs,
        }
    }

    /// Compute active window_ts from current server time
    pub fn current_window_ts(&self) -> u64 {
        let now = self.now() as u64;
        now - (now % 300)
    }

    /// Compute next window_ts
    pub fn next_window_ts(&self) -> u64 {
        self.current_window_ts() + 300
    }

    /// Seconds remaining in current window
    pub fn secs_remaining(&self) -> u64 {
        let window_end = self.current_window_ts() + 300;
        let now = self.now() as u64;
        window_end.saturating_sub(now)
    }
}
