use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::sync::{Arc, Mutex};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use crate::config::{WS_URL, FLUSH_INTERVAL_SECS, PING_INTERVAL_SECS};
use crate::database as db;
use crate::models::{PriceChangeEvent, PriceRow};
use std::time::Duration;
use tokio::time::timeout;

// --- Flush task: periodically writes buffer to database ---
fn spawn_flush_task(
    buffer: Arc<Mutex<Vec<PriceRow>>>,
    conn: Arc<Mutex<rusqlite::Connection>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(FLUSH_INTERVAL_SECS)).await;

            let rows: Vec<PriceRow> = {
                let mut buf = buffer.lock().unwrap();
                buf.drain(..).collect()
            };

            if rows.is_empty() {
                continue;
            }

            let count = rows.len();
            let db = conn.lock().unwrap();

            match db::flush_price_rows(&db, &rows) {
                Ok(_) => println!("Flushed {} rows", count),
                Err(e) => println!("Flush error: {}", e),
            }
        }
    })
}

// Type alias so we don't have to write this monster everywhere
// type WsWriter = futures_util::stream::SplitSink<
//     tokio_tungstenite::WebSocketStream<
//         tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
//     >,
//     Message,
// >;

// --- Ping task: keeps websocket alive ---
fn spawn_ping_task<S>(mut write: S) -> tokio::task::JoinHandle<()>
where
    S: SinkExt<Message> + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(PING_INTERVAL_SECS)).await;
            if write.send(Message::text("PING")).await.is_err() {
                println!("Ping failed");
                break;
            }
        }
    })
}

// --- Parse a price change event into a PriceRow ---
fn parse_price_row(
    event: &PriceChangeEvent,
    up_token: &str,
    slug: &str,
) -> Option<PriceRow> {
    if event.event_type != "price_change" || event.price_changes.len() != 2 {
        return None;
    }

    let (up_data, down_data) = if event.price_changes[0].asset_id == up_token {
        (&event.price_changes[0], &event.price_changes[1])
    } else {
        (&event.price_changes[1], &event.price_changes[0])
    };

    Some(PriceRow {
        market_slug: slug.to_string(),
        timestamp: event.timestamp.clone(),
        up_bid: up_data.best_bid.clone(),
        up_ask: up_data.best_ask.clone(),
        down_bid: down_data.best_bid.clone(),
        down_ask: down_data.best_ask.clone(),
        side: up_data.side.clone(),
        size: up_data.size.clone(),
    })
}

// --- Check if price actually changed ---
fn price_changed(row: &PriceRow, last_bid: &str, last_ask: &str) -> bool {
    row.up_bid != last_bid || row.up_ask != last_ask
}

// --- Main entry point ---
pub async fn stream_market(slug: &str, up_token: &str, down_token: &str, duration_secs: u64) {
    // Database
    let conn = db::open_connection().expect("Failed to open database");
    let conn = Arc::new(Mutex::new(conn));

    // Buffer
    let buffer: Arc<Mutex<Vec<PriceRow>>> = Arc::new(Mutex::new(Vec::new()));

    // Start background tasks
    let flush_handle = spawn_flush_task(Arc::clone(&buffer), Arc::clone(&conn));

    // Connect websocket
    let (ws_stream, _) = connect_async(WS_URL).await.expect("Failed to connect");
    println!("Connected to websocket!");

    let (write, mut read) = ws_stream.split();

    // Subscribe
    let subscribe = json!({
        "assets_ids": [up_token, down_token],
        "type": "market",
        "custom_feature_enabled": true
    });

    // Need write back briefly to send subscription before handing to ping task
    let mut write = write;
    write
        .send(Message::text(subscribe.to_string()))
        .await
        .expect("Failed to subscribe");
    println!("Subscribed to {}!", slug);

    let ping_handle = spawn_ping_task(write);

    // Listen for messages with a timeout
    let ws_buffer = Arc::clone(&buffer);
    let slug = slug.to_string();
    let up = up_token.to_string();
    let mut last_up_bid = String::new();
    let mut last_up_ask = String::new();
    let mut skipped: u64 = 0;

    let stream_duration = Duration::from_secs(duration_secs);
    let start_time = tokio::time::Instant::now();

    loop {
        let remaining = stream_duration.saturating_sub(start_time.elapsed());
        if remaining.is_zero() {
            println!("Window ended. Disconnecting.");
            break;
        }

        match timeout(remaining, read.next()).await {
            Ok(Some(msg)) => match msg {
                Ok(Message::Text(text)) => {
                    let text = text.to_string();
                    if text == "PONG" {
                        continue;
                    }

                    if let Ok(event) = serde_json::from_str::<PriceChangeEvent>(&text) {
                        if let Some(row) = parse_price_row(&event, &up, &slug) {
                            if !price_changed(&row, &last_up_bid, &last_up_ask) {
                                skipped += 1;
                                continue;
                            }

                            last_up_bid = row.up_bid.clone();
                            last_up_ask = row.up_ask.clone();

                            let mut buf = ws_buffer.lock().unwrap();
                            buf.push(row);
                        }
                    }
                }
                Ok(Message::Close(_)) => {
                    println!("Connection closed (skipped {} duplicates)", skipped);
                    break;
                }
                Err(e) => {
                    println!("Error: {} (skipped {} duplicates)", e, skipped);
                    break;
                }
                _ => {}
            },
            Ok(None) => {
                println!("Stream ended");
                break;
            }
            Err(_) => {
                println!("Window time expired. Disconnecting.");
                break;
            }
        }
    }

    // Final flush — grab anything left in the buffer
    let remaining_rows: Vec<PriceRow> = {
        let mut buf = buffer.lock().unwrap();
        buf.drain(..).collect()
    };
    if !remaining_rows.is_empty() {
        let db = conn.lock().unwrap();
        match db::flush_price_rows(&db, &remaining_rows) {
            Ok(_) => println!("Final flush: {} rows", remaining_rows.len()),
            Err(e) => println!("Final flush error: {}", e),
        }
    }

    ping_handle.abort();
    flush_handle.abort();
    println!("Done! Skipped {} duplicates total", skipped);
}