import sqlite3
import pandas as pd
import matplotlib.pyplot as plt


def load_data(db_path="polymarket.db"):
    conn = sqlite3.connect(db_path)
    poly_df = pd.read_sql_query("SELECT * FROM price_changes", conn)
    btc_df = pd.read_sql_query("SELECT * FROM btc_prices", conn)
    conn.close()

    poly_df["up_bid"] = poly_df["up_bid"].astype(float)
    poly_df["up_ask"] = poly_df["up_ask"].astype(float)
    poly_df["down_bid"] = poly_df["down_bid"].astype(float)
    poly_df["down_ask"] = poly_df["down_ask"].astype(float)
    poly_df["timestamp"] = pd.to_numeric(poly_df["timestamp"])

    btc_df["mark_price"] = btc_df["mark_price"].astype(float)
    btc_df["index_price"] = btc_df["index_price"].astype(float)
    btc_df["timestamp"] = pd.to_numeric(btc_df["timestamp"])

    return poly_df, btc_df


def plot_price_and_spread(window, slug):
    fig, axes = plt.subplots(2, 1, figsize=(14, 8), sharex=True)
    fig.suptitle(f"Market: {slug}", fontsize=14)

    axes[0].plot(window["seconds"], window["up_bid"], label="Up Bid", alpha=0.8)
    axes[0].plot(window["seconds"], window["up_ask"], label="Up Ask", alpha=0.8)
    axes[0].set_ylabel("Price")
    axes[0].set_title("Up Token Price Over Time")
    axes[0].legend()
    axes[0].grid(True, alpha=0.3)

    spread = window["up_ask"] - window["up_bid"]
    axes[1].plot(window["seconds"], spread, color="red", alpha=0.8)
    axes[1].set_ylabel("Spread")
    axes[1].set_xlabel("Seconds into window")
    axes[1].set_title("Bid-Ask Spread Over Time")
    axes[1].grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(f"analysis/{slug}_price_spread.png", dpi=150)
    plt.show()


def plot_up_vs_down(window, slug):
    fig, ax = plt.subplots(figsize=(14, 5))
    ax.plot(window["seconds"], window["up_bid"], label="Up Bid", alpha=0.8)
    ax.plot(window["seconds"], window["down_bid"], label="Down Bid", alpha=0.8)
    ax.set_ylabel("Price")
    ax.set_xlabel("Seconds into window")
    ax.set_title(f"Up vs Down Token Price - {slug}")
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"analysis/{slug}_updown.png", dpi=150)
    plt.show()


def plot_polymarket_vs_btc(window, btc_window, slug, price_to_beat):
    fig, ax1 = plt.subplots(figsize=(14, 6))

    # Polymarket Up bid on left axis
    ax1.plot(window["seconds"], window["up_bid"], label="Up Bid (Polymarket)", color="blue", alpha=0.8)
    ax1.set_ylabel("Polymarket Up Price", color="blue")
    ax1.set_xlabel("Seconds into window")
    ax1.tick_params(axis="y", labelcolor="blue")

    # BTC price on right axis
    ax2 = ax1.twinx()
    ax2.plot(btc_window["seconds"], btc_window["index_price"], label="BTC Index Price (Binance)", color="orange", alpha=0.8)
    ax2.set_ylabel("BTC Price (USD)", color="orange")
    ax2.tick_params(axis="y", labelcolor="orange")

    # Price to beat reference line
    if price_to_beat > 0:
        ax2.axhline(y=price_to_beat, color="red", linestyle="--", alpha=0.6, label=f"Price to Beat: ${price_to_beat:,.2f}")

    # Combine legends
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")

    ax1.set_title(f"Polymarket Odds vs BTC Price - {slug}")
    ax1.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"analysis/{slug}_overlay.png", dpi=150)
    plt.show()


def main():
    poly_df, btc_df = load_data()
    slugs = poly_df["market_slug"].unique()
    print(f"Found {len(slugs)} market windows: {slugs}")

    # Load events for price_to_beat
    conn = sqlite3.connect("polymarket.db")
    events_df = pd.read_sql_query("SELECT * FROM events", conn)
    conn.close()

    for slug in slugs:
        window = poly_df[poly_df["market_slug"] == slug].copy()
        min_ts = window["timestamp"].min()
        window["seconds"] = (window["timestamp"] - min_ts) / 1000

        # Filter BTC data to same time range
        max_ts = window["timestamp"].max()
        btc_window = btc_df[(btc_df["timestamp"] >= min_ts) & (btc_df["timestamp"] <= max_ts)].copy()
        btc_window["seconds"] = (btc_window["timestamp"] - min_ts) / 1000

        # Get price to beat
        event_row = events_df[events_df["market_slug"] == slug]
        price_to_beat = event_row["price_to_beat"].values[0] if not event_row.empty else 0

        plot_price_and_spread(window, slug)
        plot_up_vs_down(window, slug)

        if not btc_window.empty:
            plot_polymarket_vs_btc(window, btc_window, slug, price_to_beat)
            print(f"BTC price range: ${btc_window['index_price'].min():,.2f} - ${btc_window['index_price'].max():,.2f}")
            print(f"Price to beat: ${price_to_beat:,.2f}")
        else:
            print(f"No BTC data available for {slug}")


if __name__ == "__main__":
    main()