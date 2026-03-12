#![allow(dead_code)]

use rusqlite::{Connection, Result};

fn main() -> Result<()> {
    let conn = Connection::open("polymarket.db")?;

    // Events table - stores metadata from the REST API
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

    // Price chnages table - stores websocket data
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
            size TEXT NOT NULL,
            FOREIGN KEY (market_slug) REFERENCES events(market_slug)
        )",
        [],
    )?;

    conn.execute("CREATE INDEX IF NOT EXISTS idx_slug ON price_changes(market_slug)", [])?;
    conn.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON price_changes(timestamp)", [])?;

    println!("Tables created!");

    conn.execute(
        "INSERT OR IGNORE INTO events (market_slug, title, start_time, end_time, price_to_beat, up_token_id, down_token_id)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        [
            "btc-updown-5m-1772920200",
            "Bitcoin Up or Down - March 7, 4:50PM-4:55PM ET",
            "2026-03-07T21:50:00Z",
            "2026-03-07T21:55:00Z",
            "67366.25",
            "65903955576131251975456924178873014958148067206641217031497590052493746156525",
            "111216110701393155519418828279698073650276880812558103215740677486839793443855",
        ],
    )?;

    println!("Dummy event inserted!");

    // Insert dummy price changes matching the websocket structure
    conn.execute(
        "INSERT INTO price_changes (market_slug, timestamp, up_bid, up_ask, down_bid, down_ask, side, size)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        [
            "btc-updown-5m-1772920200",
            "1772923411657",
            "0.74",
            "0.75",
            "0.25",
            "0.26",
            "SELL",
            "155.71",
        ],
    )?;

    conn.execute(
        "INSERT INTO price_changes (market_slug, timestamp, up_bid, up_ask, down_bid, down_ask, side, size)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        [
            "btc-updown-5m-1772920200",
            "1772923411700",
            "0.76",
            "0.78",
            "0.22",
            "0.24",
            "BUY",
            "200.50",
        ],
    )?;

    println!("Dummy price changes inserted!");


    // Query it back
    let mut stmt = conn.prepare(
        "SELECT p.timestamp, p.up_bid, p.up_ask, p.down_bid, p.down_ask, p.side, p.size, e.title
         FROM price_changes p
         JOIN events e ON p.market_slug = e.market_slug
         WHERE p.market_slug = ?1"
    )?;

    let rows = stmt.query_map(["btc-updown-5m-1772920200"], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, String>(5)?,
            row.get::<_, String>(6)?,
            row.get::<_, String>(7)?,
        ))
    })?;

    for row in rows {
        let (ts, up_bid, up_ask, down_bid, down_ask, side, size, title) = row?;
        println!("{}", title);
        println!(
            "  ts: {} | UP bid/ask: {}/{} | DOWN bid/ask: {}/{} | {} {}",
            ts, up_bid, up_ask, down_bid, down_ask, side, size
        );
    }

    Ok(())
}