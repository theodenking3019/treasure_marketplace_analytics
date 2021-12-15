CREATE TABLE IF NOT EXISTS marketplace_listings (
    tx_hash TEXT PRIMARY KEY,
    timestamp TEXT,
    wallet_seller TEXT,
    listing_amt_magic REAL,
    gas_fee_eth REAL,
    nft_collection TEXT,
    nft_id INTEGER,
    quantity INTEGER,
    FOREIGN KEY(sale_tx) REFERENCES marketplace_sales(tx_hash),
    FOREIGN KEY(cancellation_tx) REFERENCES marketplace_listings_cancellations(tx_hash),
    sold_at TEXT,
    updated_at TEXT,
    cancelled_at TEXT
);