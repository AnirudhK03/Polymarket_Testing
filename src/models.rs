#![allow(dead_code)]
use serde::Deserialize;

// -- From REST API --
#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    pub id: String,
    pub slug: String,
    pub title: String,
    pub active: bool,
    pub closed: bool,
    pub volume: f64,
    pub start_time: Option<String>,
    pub markets: Vec<Market>,
    pub event_metadata: Option<EventMetadata>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Market {
    pub id: String,
    pub slug: String,
    pub outcomes: String,
    pub outcome_prices: String,
    pub clob_token_ids: String,
    pub active: bool,
    pub closed: bool,
    pub last_trade_price: Option<f64>,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub volume_num: f64,
    pub end_date: String,
    pub event_start_time: Option<String>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct EventMetadata {
    pub price_to_beat: Option<f64>,
}

// -- From Websocket --
#[derive(Deserialize, Debug)]
pub struct PriceChangeEvent {
    pub market: String,
    pub price_changes: Vec<PriceChange>,
    pub timestamp: String,
    pub event_type: String,
}

#[derive(Deserialize, Debug)]
pub struct PriceChange {
    pub asset_id: String,
    pub price: String,
    pub size: String,
    pub side: String,
    pub best_bid: String,
    pub best_ask: String,
}

// -- What we store in DB --
#[derive(Debug, Clone)]
pub struct PriceRow {
    pub market_slug: String,
    pub timestamp: String,
    pub up_bid: String,
    pub up_ask: String,
    pub down_bid: String,
    pub down_ask: String,
    pub side: String,
    pub size: String,
}

// -- Binance data --
#[derive(Deserialize, Debug)]
pub struct BinanceMarkPrice {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "E")]
    pub event_time: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "p")]
    pub mark_price: String,
    #[serde(rename = "i")]
    pub index_price: String,
}

// -- What we store in DB --
#[derive(Debug, Clone)]
pub struct BtcPriceRow {
    pub market_slug: String,
    pub timestamp: String,
    pub mark_price: String,
    pub index_price: String,
}