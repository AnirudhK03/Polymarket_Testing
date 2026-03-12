fn main() {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let current_window = (now / 300) * 300;
    let next_window = current_window + 300;
    let seconds_until_next = next_window - now;

    // The window we'd connect to
    let target_window = next_window;
    // When that window ends
    let target_end = target_window + 300;
    // How long from now until it ends
    let total_wait = target_end - now;

    println!("Current time:          {}", now);
    println!("Next window starts:    {} (in {}s)", next_window, seconds_until_next);
    println!("That window ends:      {} (in {}s)", target_end, total_wait);
    println!();
    println!("Plan:");
    println!("  1. Sleep {}s until window starts", seconds_until_next + 3);
    println!("  2. Fetch event for btc-updown-5m-{}", target_window);
    println!("  3. Stream for ~297s");
    println!("  4. Disconnect, move to btc-updown-5m-{}", target_window + 300);
}