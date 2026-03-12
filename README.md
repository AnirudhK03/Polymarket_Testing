# Polymarket BTC Up/Down Trading Bot

A real-time data pipeline for collecting and analyzing Polymarket's 5-minute BTC Up/Down prediction markets, built in Rust with Python analysis tools.

## What This Does

- Fetches active BTC Up/Down market events from Polymarket's Gamma API
- Streams real-time orderbook price changes via Polymarket's websocket
- Streams real-time BTC price data via Binance's websocket (mark price, 1s interval)
- Stores all data in a local SQLite database with duplicate filtering
- Automatically rotates to the next 5-minute market window
- Python scripts for visualizing price movements, spreads, and Polymarket vs BTC price correlation

## Project Structure

```
src/
  main.rs              # Orchestrator: API fetch → websocket stream → DB
  config.rs            # URLs, endpoints, timing constants
  models.rs            # Shared data structs (Event, Market, PriceRow, etc.)
  market_api.rs        # Polymarket Gamma API client + price-to-beat poller
  database.rs          # SQLite setup, table creation, batch inserts
  websocket.rs         # Polymarket websocket (price changes with duplicate filter)
  finance_websocket.rs # Binance websocket (BTC mark/index price)
  bin/
    api.rs             # Standalone test: API call
    db.rs              # Standalone test: database operations
    ws.rs              # Standalone test: websocket connection
    timetest.rs        # Standalone test: window timing math
analysis/
  visualize.py         # Price over time, spread, Up vs Down, BTC overlay charts
```

## Prerequisites

- **Rust** (installed via rustup): https://rustup.rs
- **Python 3.11+** (via Conda or venv)
- No system SQLite needed — bundled via `rusqlite`

## Setup

### Rust

```bash
git clone https://github.com/YOUR_USERNAME/YOUR_REPO.git
cd YOUR_REPO
cargo build
```

### Python (using Conda)

```bash
conda create -n polymarket python=3.11
conda activate polymarket
conda install pandas matplotlib plotly
```

## Usage

### Collect Data

Update the timestamp in `src/config.rs` if needed, then:

```bash
cargo run
```

The pipeline will:
1. Calculate the next 5-minute window
2. Sleep until it starts (+3s buffer)
3. Fetch the event and token IDs from Polymarket API
4. Stream Polymarket price changes and Binance BTC price concurrently
5. Flush data to SQLite every 2 seconds
6. Disconnect when the window ends and rotate to the next one

Use `caffeinate cargo run` on macOS to prevent sleep during long collection runs.

### Run Standalone Tests

```bash
cargo run --bin api        # Test API call
cargo run --bin db         # Test database operations
cargo run --bin ws         # Test websocket connection
cargo run --bin timetest   # Test window timing math
```

### Visualize Data

```bash
conda activate polymarket
python analysis/visualize.py
```

Generates charts for each collected market window:
- Up token bid/ask price over time
- Bid-ask spread over time
- Up vs Down token price comparison
- Polymarket odds vs BTC price overlay with price-to-beat reference line

### Query the Database

```bash
# Count events and price changes
sqlite3 polymarket.db "SELECT market_slug, COUNT(*) FROM price_changes GROUP BY market_slug;"
sqlite3 polymarket.db "SELECT market_slug, COUNT(*) FROM btc_prices GROUP BY market_slug;"

# Check price to beat
sqlite3 polymarket.db "SELECT market_slug, price_to_beat FROM events;"

# Sample data
sqlite3 polymarket.db "SELECT timestamp, up_bid, up_ask FROM price_changes LIMIT 10;"
```

## Database Schema

**events** — One row per market window (from REST API)
- `market_slug`, `title`, `start_time`, `end_time`, `price_to_beat`, `up_token_id`, `down_token_id`

**price_changes** — Polymarket websocket data (deduplicated, only stored when price changes)
- `market_slug`, `timestamp`, `up_bid`, `up_ask`, `down_bid`, `down_ask`, `side`, `size`

**btc_prices** — Binance websocket data (1 update per second)
- `market_slug`, `timestamp`, `mark_price`, `index_price`

## Key Design Decisions

- **Duplicate filtering**: Only stores Polymarket price changes when bid/ask actually changes. Reduces ~30,000 raw messages per window to ~1,000-2,000 meaningful data points.
- **Buffered writes**: Websocket data is buffered in memory and flushed to SQLite every 2 seconds in batch transactions for performance.
- **Cancellation tokens**: When the Polymarket websocket ends, Binance stream stops automatically via `tokio::CancellationToken`.
- **Rust for collection, Python for analysis**: Each tool doing what it does best.

## Dependencies (Rust)

- `reqwest` — HTTP client
- `tokio` — Async runtime
- `tokio-tungstenite` — Websocket client
- `serde` / `serde_json` — JSON serialization
- `rusqlite` (bundled) — SQLite database
- `futures-util` — Stream utilities
- `tokio-util` — Cancellation tokens

## Next Steps

- [ ] Deploy to EC2 for continuous multi-day data collection
- [ ] Implement Black-Scholes binary option pricing model
- [ ] Build volatility model (GARCH) for fair price estimation
- [ ] Backtest taker strategy against historical data
- [ ] Graduate to market making with live order placement