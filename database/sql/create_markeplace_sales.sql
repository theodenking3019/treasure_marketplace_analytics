CREATE TABLE IF NOT EXISTS marketplace_sales (
    tx_hash TEXT PRIMARY KEY,
    timestamp TEXT,
    wallet_buyer TEXT,
    wallet_seller TEXT,
    sale_amt_magic REAL,
    seller_amt_received_magic REAL,
    dao_amt_received_magic REAL,
    gas_fee_eth REAL,
    nft_collection TEXT,
    nft_id INTEGER,
    nft_name TEXT,
    nft_subcategory TEXT,
    quantity INTEGER
);