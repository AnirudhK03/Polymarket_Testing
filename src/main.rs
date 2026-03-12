mod config;
mod models;
mod market_api;
mod database;
mod websocket;
mod binance_websocket;

use tokio_util::sync::CancellationToken;
use config::MARKET_SLUG;

#[tokio::main]
async fn main() {
    let buffer_secs = 3;

    loop {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let current_window = (now / 300) * 300;
        let next_window = current_window + 300;
        let sleep_secs = (next_window - now) + buffer_secs;

        println!("=== Next window: {}-{} ===", MARKET_SLUG, next_window);
        println!("Sleeping {}s until window starts...", sleep_secs);

        tokio::time::sleep(tokio::time::Duration::from_secs(sleep_secs)).await;

        // Fetch event
        let time = next_window.to_string();
        println!("Fetching event...");
        let events = match market_api::fetch_event(&time).await {
            Ok(e) => e,
            Err(e) => {
                println!("API error: {}. Retrying next window.", e);
                continue;
            }
        };

        if events.is_empty() {
            println!("No events found. Skipping to next window.");
            continue;
        }

        let event = &events[0];
        let market = &event.markets[0];

        let token_ids: Vec<String> = serde_json::from_str(&market.clob_token_ids)
            .expect("Failed to parse token IDs");
        let up_token = &token_ids[0];
        let down_token = &token_ids[1];

        println!("Event: {}", event.title);
        println!("Slug: {}", event.slug);

        // Save event to DB
        let conn = database::open_connection().expect("Failed to open database");
        let price_to_beat = event
            .event_metadata
            .as_ref()
            .and_then(|m| m.price_to_beat)
            .unwrap_or(0.0);

        let _ = database::insert_event(
            &conn,
            &event.slug,
            &event.title,
            &event.start_time.clone().unwrap_or_default(),
            &market.end_date,
            price_to_beat,
            up_token,
            down_token,
        );

        // Calculate remaining time in window
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let window_end = next_window + 300;
        let stream_duration = window_end.saturating_sub(now);

        println!("Streaming for ~{}s...", stream_duration);

        // Run all three tasks concurrently with cancellation
        let slug = event.slug.clone();
        let up = up_token.clone();
        let down = down_token.clone();
        let cancel = CancellationToken::new();

        let poly_cancel = cancel.clone();
        let binance_cancel = cancel.clone();

        tokio::join!(
            async {
                websocket::stream_market(&slug, &up, &down, stream_duration).await;
                poly_cancel.cancel();
                println!("Polymarket ended, cancelling other tasks...");
            },
            async {
                binance_websocket::stream_btc_price(&slug, stream_duration, binance_cancel).await;
            },
            market_api::poll_price_to_beat(&slug, &time)
        );

        println!("=== Window complete ===\n");
    }
}