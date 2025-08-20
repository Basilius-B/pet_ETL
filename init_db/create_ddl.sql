CREATE TABLE IF NOT EXISTS bitcoin_prices (
            id SERIAL PRIMARY KEY,
            price_usd FLOAT,
            timestamp TIMESTAMPTZ
        );

