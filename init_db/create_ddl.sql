CREATE TABLE IF NOT EXISTS bitcoin_prices (
            id SERIAL PRIMARY KEY,
            price_usd FLOAT,
            timestamp TIMESTAMPTZ
        );


CREATE TABLE IF NOT EXISTS BTCUSDT_5m_kline(
    open_time TIMESTAMP,
    open NUMERIC(18, 4),
    high NUMERIC(18, 4),
    low NUMERIC(18, 4),
    close NUMERIC(18, 4),
    volume NUMERIC(20, 6),
    close_time TIMESTAMP,
    quote_volume NUMERIC(20, 6),
    count INTEGER,
    taker_buy_volume NUMERIC(20, 6),
    taker_buy_quote_volume NUMERIC(20, 6),
    ignore INTEGER
);
