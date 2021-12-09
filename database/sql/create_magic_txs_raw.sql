CREATE TABLE IF NOT EXISTS magic_txs_raw (
    blockNumber INT,
    timeStamp INT,
    hash TEXT,
    nonce INT,
    blockHash TEXT,
    from_wallet TEXT,
    contractAddress TEXT,
    to_wallet TEXT,
    tx_value REAL,
    tokenName TEXT,
    tokenSymbol TEXT,
    tokenDecimal INT,
    transactionIndex REAL,
    gas INT,
    gasPrice INT,
    gasUsed INT,
    cumulativeGasUsed TEXT,
    input TEXT,
    confirmations INT
);