use rusqlite::{Connection, Result};
use crate::config::DB_PATH;
use crate::models::{PriceRow, BtcPriceRow};

pub fn open_connection() -> Result<Connection> {
    let conn = Connection::open(DB_PATH)?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            market_slug TEXT NOT NULL UNIQUE,
            title TEXT,
            start_time TEXT,
            end_time TEXT,
            price_to_beat REAL,
            up_token_id TEXT NOT NULL,
            down_token_id TEXT NOT NULL
        )",
        [],
    )?;

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
    )?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS btc_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            market_slug TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            mark_price TEXT NOT NULL,
            index_price TEXT NOT NULL
        )",
        [],
    )?;

    conn.execute("CREATE INDEX IF NOT EXISTS idx_slug ON price_changes(market_slug)", [])?;
    conn.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON price_changes(timestamp)", [])?;
    conn.execute("CREATE INDEX IF NOT EXISTS idx_btc_timestamp ON btc_prices(timestamp)", [])?;

    Ok(conn)
}

pub fn insert_event(
    conn: &Connection,
    slug: &str,
    title: &str,
    start_time: &str,
    end_time: &str,
    price_to_beat: f64,
    up_token: &str,
    down_token: &str,
) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO events (market_slug, title, start_time, end_time, price_to_beat, up_token_id, down_token_id)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        rusqlite::params![slug, title, start_time, end_time, price_to_beat, up_token, down_token],
    )?;
    Ok(())
}

pub fn flush_price_rows(conn: &Connection, rows: &[PriceRow]) -> Result<()> {
    conn.execute("BEGIN", [])?;
    for row in rows {
        conn.execute(
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
        )?;
    }
    conn.execute("COMMIT", [])?;
    Ok(())
}

pub fn flush_btc_prices(conn: &Connection, rows: &[BtcPriceRow]) -> Result<()> {
    conn.execute("BEGIN", [])?;
    for row in rows {
        conn.execute(
            "INSERT INTO btc_prices (market_slug, timestamp, mark_price, index_price)
             VALUES (?1, ?2, ?3, ?4)",
            [
                &row.market_slug,
                &row.timestamp,
                &row.mark_price,
                &row.index_price,
            ],
        )?;
    }
    conn.execute("COMMIT", [])?;
    Ok(())
}