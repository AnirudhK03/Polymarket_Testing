use crate::config::{BASE_URL, MARKET_SLUG};
use crate::models::Event;
use crate::database;

pub async fn fetch_event(time: &str) -> Result<Vec<Event>, reqwest::Error> {
    let url = format!("{}/events?slug={}-{}", BASE_URL, MARKET_SLUG, time);
    let events: Vec<Event> = reqwest::get(&url)
        .await?
        .json()
        .await?;
    Ok(events)
}

pub async fn poll_price_to_beat(slug: &str, time: &str) {
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

        match fetch_event(time).await {
            Ok(events) => {
                if let Some(event) = events.first() {
                    if let Some(meta) = &event.event_metadata {
                        if let Some(ptb) = meta.price_to_beat {
                            if ptb > 0.0 {
                                let conn = database::open_connection().expect("Failed to open db");
                                conn.execute(
                                    "UPDATE events SET price_to_beat = ?1 WHERE market_slug = ?2",
                                    rusqlite::params![ptb, slug],
                                ).ok();
                                println!("[PriceToBeat] Updated: ${:.2}", ptb);
                                return;
                            }
                        }
                    }
                }
                println!("[PriceToBeat] Not available yet, retrying in 10s...");
            }
            Err(e) => {
                println!("[PriceToBeat] API error: {}, retrying in 10s...", e);
            }
        }
    }
}