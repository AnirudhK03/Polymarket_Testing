#![allow(dead_code, unused_variables)]

use futures_util::{SinkExt, StreamExt};
use rusqlite::Connection;
use serde::Deserialize;
use serde_json::json;
use std::sync::{Arc, Mutex};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

#[derive(Deserialize, Debug)]
struct PriceChangeEvent {
    market: String,
    price_changes: Vec<PriceChange>,
    timestamp: String,
    event_type: String,
}

#[derive(Deserialize, Debug)]
struct PriceChange {
    asset_id: String,
    price: String,
    size: String,
    side: String,
    best_bid: String,
    best_ask: String,
}

// -- What we actually store --
#[derive(Debug, Clone)]
struct PriceRow {
    market_slug: String,
    timestamp: String,
    up_bid: String,
    up_ask: String,
    down_bid: String,
    down_ask: String,
    side: String,
    size: String,
}

#[tokio::main]
async fn main() {
    // --- CONFIG ---
    // Swap these out for each market you want to track
    let market_slug = "btc-updown-15m-1772935200";
    let up_token = "104908782711505916641970732141209083608267572986679412028690686117843340068098";
    let down_token = "47382876928808361041658382385775856793651658342091942822771225967291768536460";
    let batch_size = 500;
    let flush_interval_secs = 2;

    // --- DATABASE ---
    let conn = Connection::open("polymarket.db").expect("Failed to open database");
    conn.execute(
        "CREATE TABLE IF NOT EXISTS price_changes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            market_slug TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            up_bid TEXT NOT NULL,
            up_ask TEXT NOT NULL,
            down_bid TEXT NOT NULL,
            down_ask TEXT NOT NULL,
            side TEXT NOT NULL,
            size TEXT NOT NULL
        )",
        [],
    )
    .expect("Failed to create table");

    let conn = Arc::new(Mutex::new(conn));

    // --- BUFFER ---
    // Shared between the websocket listener and the flush task
    let buffer: Arc<Mutex<Vec<PriceRow>>> = Arc::new(Mutex::new(Vec::new()));

    // --- FLUSH TASK ---
    // Periodically writes buffered data to SQLite
    let flush_buffer = Arc::clone(&buffer);
    let flush_conn = Arc::clone(&conn);
    let flush_handle = tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(flush_interval_secs)).await;

            let rows: Vec<PriceRow> = {
                let mut buf = flush_buffer.lock().unwrap();
                buf.drain(..).collect()
            };

            if rows.is_empty() {
                continue;
            }

            let count = rows.len();
            let db = flush_conn.lock().unwrap();

            // Batch insert inside a transaction for speed
            db.execute("BEGIN", []).ok();
            for row in &rows {
                db.execute(
                    "INSERT INTO price_changes (market_slug, timestamp, up_bid, up_ask, down_bid, down_ask, side, size)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    [
                        &row.market_slug,
                        &row.timestamp,
                        &row.up_bid,
                        &row.up_ask,
                        &row.down_bid,
                        &row.down_ask,
                        &row.side,
                        &row.size,
                    ],
                )
                .ok();
            }
            db.execute("COMMIT", []).ok();

            println!("Flushed {} rows to database", count);
        }
    });

    // --- WEBSOCKET ---
    let url = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
    let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");
    println!("Connected to websocket!");

    let (mut write, mut read) = ws_stream.split();

    // Send subscription
    let subscribe = json!({
        "assets_ids": [up_token, down_token],
        "type": "market",
        "custom_feature_enabled": true
    });

    write
        .send(Message::text(subscribe.to_string()))
        .await
        .expect("Failed to subscribe");
    println!("Subscribed!");

    // --- PING TASK ---
    let ping_handle = tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
            if write.send(Message::text("PING".to_string())).await.is_err() {
                println!("Ping failed, connection lost");
                break;
            }
        }
    });

    // --- LISTEN ---
    let ws_buffer = Arc::clone(&buffer);
    let slug = market_slug.to_string();
    let up = up_token.to_string();

    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                if text == "PONG" {
                    continue;
                }

                // Try parsing as price change
                if let Ok(event) = serde_json::from_str::<PriceChangeEvent>(&text) {
                    if event.event_type == "price_change" && event.price_changes.len() == 2 {
                        // Figure out which is Up and which is Down
                        let (up_data, down_data) = if event.price_changes[0].asset_id == up {
                            (&event.price_changes[0], &event.price_changes[1])
                        } else {
                            (&event.price_changes[1], &event.price_changes[0])
                        };

                        let row = PriceRow {
                            market_slug: slug.clone(),
                            timestamp: event.timestamp.clone(),
                            up_bid: up_data.best_bid.clone(),
                            up_ask: up_data.best_ask.clone(),
                            down_bid: down_data.best_bid.clone(),
                            down_ask: down_data.best_ask.clone(),
                            side: up_data.side.clone(),
                            size: up_data.size.clone(),
                        };

                        // Push into buffer — this is fast, no disk IO
                        let mut buf = ws_buffer.lock().unwrap();
                        buf.push(row);
                    }
                }
            }
            Ok(Message::Close(_)) => {
                println!("Connection closed by server");
                break;
            }
            Err(e) => {
                println!("Error: {}", e);
                break;
            }
            _ => {}
        }
    }

    // Cleanup
    ping_handle.abort();
    flush_handle.abort();
    println!("Done!");
}
