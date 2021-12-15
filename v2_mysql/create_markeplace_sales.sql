CREATE TABLE IF NOT EXISTS marketplace_sales (
    tx_hash VARCHAR(255),
    datetime DATETIME NOT NULL,
    wallet_buyer VARCHAR(255) NOT NULL,
    wallet_seller VARCHAR(255) NOT NULL,
    sale_amt_magic FLOAT NOT NULL,
    seller_amt_received_magic FLOAT NOT NULL,
    dao_amt_received_magic FLOAT NOT NULL,
    gas_fee_eth DOUBLE NOT NULL,
    nft_collection VARCHAR(255),
    nft_id INT,
    nft_name VARCHAR(255),
    nft_subcategory VARCHAR(255),
    quantity INT NOT NULL,
    PRIMARY KEY (tx_hash)
);