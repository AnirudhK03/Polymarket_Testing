#![allow(dead_code, unused_variables, unused_imports)]

use reqwest;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Event {
    id: String,
    slug: String,
    title: String,
    active: bool,
    closed: bool,
    volume: f64,
    start_time: Option<String>,
    markets: Vec<Market>,
    event_metadata: Option<EventMetadata>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Market {
    id: String,
    slug: String,
    outcomes: String,           // comes as a JSON string like "[\"Up\", \"Down\"]"
    outcome_prices: String,     // same, "[\"1\", \"0\"]"
    clob_token_ids: String,     // the token IDs you'll need for websocket
    active: bool,
    closed: bool,
    last_trade_price: Option<f64>,
    best_bid: Option<f64>,
    best_ask: Option<f64>,
    volume_num: f64,
    end_date: String,
    event_start_time: Option<String>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct EventMetadata {
    price_to_beat: Option<f64>,
}


#[tokio::main]
async fn main() -> Result<(), reqwest::Error> {
    let base_url = "https://gamma-api.polymarket.com";
    let time = "1772920200";
    let url = format!("{}/events?slug=btc-updown-5m-{}", base_url, time);

    let response: Vec<Event> = reqwest::get(&url)
        .await?
        .json()
        .await?;

    print!("{:#?}", response);
    Ok(())
}