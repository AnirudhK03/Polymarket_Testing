use futures_util::{SinkExt, StreamExt};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::timeout;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use crate::config::FLUSH_INTERVAL_SECS;
use crate::database as db;
use crate::models::{BinanceMarkPrice, BtcPriceRow};

const BINANCE_WS_URL: &str = "wss://fstream.binance.com/ws/btcusdt@markPrice@1s";

fn spawn_flush_task(
    buffer: Arc<Mutex<Vec<BtcPriceRow>>>,
    conn: Arc<Mutex<rusqlite::Connection>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(FLUSH_INTERVAL_SECS)).await;

            let rows: Vec<BtcPriceRow> = {
                let mut buf = buffer.lock().unwrap();
                buf.drain(..).collect()
            };

            if rows.is_empty() {
                continue;
            }

            let count = rows.len();
            let db = conn.lock().unwrap();

            match db::flush_btc_prices(&db, &rows) {
                Ok(_) => println!("[Binance] Flushed {} rows", count),
                Err(e) => println!("[Binance] Flush error: {}", e),
            }
        }
    })
}

pub async fn stream_btc_price(slug: &str, duration_secs: u64, cancel: tokio_util::sync::CancellationToken) {
    // Database
    let conn = db::open_connection().expect("Failed to open database");
    let conn = Arc::new(Mutex::new(conn));

    // Buffer
    let buffer: Arc<Mutex<Vec<BtcPriceRow>>> = Arc::new(Mutex::new(Vec::new()));

    // Flush task
    let flush_handle = spawn_flush_task(Arc::clone(&buffer), Arc::clone(&conn));

    // Connect — no subscription needed, data flows immediately
    let (ws_stream, _) = connect_async(BINANCE_WS_URL)
        .await
        .expect("Failed to connect to Binance");
    println!("[Binance] Connected!");

    let (write, mut read) = ws_stream.split();

    // Binance sends ping frames automatically, tungstenite handles pong replies
    // But we keep write alive so the connection doesn't drop
    let _write = write;

    // Listen
    let start_time = tokio::time::Instant::now();
    let stream_duration = Duration::from_secs(duration_secs);

    loop {
        let remaining = stream_duration.saturating_sub(start_time.elapsed());
        if remaining.is_zero() {
            println!("[Binance] Window ended. Disconnecting.");
            break;
        }

        tokio::select! {
            _ = cancel.cancelled() => {
                println!("[Binance] Cancelled by Polymarket ending.");
                break;
            }
            msg = timeout(remaining, read.next()) => {
                match msg {
                    Ok(Some(msg)) => match msg {
                        Ok(Message::Text(text)) => {
                            let text = text.to_string();

                            if let Ok(data) = serde_json::from_str::<BinanceMarkPrice>(&text) {
                                let row = BtcPriceRow {
                                    market_slug: slug.to_string(),
                                    timestamp: data.event_time.to_string(),
                                    mark_price: data.mark_price,
                                    index_price: data.index_price,
                                };

                                let mut buf = buffer.lock().unwrap();
                                buf.push(row);
                            }
                        }
                        Ok(Message::Close(_)) => {
                            println!("[Binance] Connection closed");
                            break;
                        }
                        Err(e) => {
                            println!("[Binance] Error: {}", e);
                            break;
                        }
                        _ => {}
                    },
                    Ok(None) => {
                        println!("[Binance] Stream ended");
                        break;
                    }
                    Err(_) => {
                        println!("[Binance] Window time expired. Disconnecting.");
                        break;
                    }
                }
            }
        }
    }

    // Final flush
    let remaining_rows: Vec<BtcPriceRow> = {
        let mut buf = buffer.lock().unwrap();
        buf.drain(..).collect()
    };
    if !remaining_rows.is_empty() {
        let db = conn.lock().unwrap();
        match db::flush_btc_prices(&db, &remaining_rows) {
            Ok(_) => println!("[Binance] Final flush: {} rows", remaining_rows.len()),
            Err(e) => println!("[Binance] Final flush error: {}", e),
        }
    }

    flush_handle.abort();
    println!("[Binance] Done!");
}