CREATE OR REPLACE VIEW agg_daily_vol_by_collection (
    date,
    nft_collection,
    volume_magic,
    volume_usd,
    volume_eth,
    n_sales, 
    avg_sale_amt_magic,
    avg_sale_amt_usd,
    avg_sale_amt_eth
)
AS 
WITH staging_1 AS (
	SELECT 
		a.tx_hash,
        b.datetime
	FROM marketplace_sales a 
	LEFT JOIN token_prices b ON a.datetime <= b.datetime
),
staging_2 AS (
	SELECT 
		tx_hash, 
        MIN(datetime) AS closest_price_time
	FROM staging_1 
    GROUP BY 1
)
SELECT 
	DATE(a.datetime) AS date, 
    nft_collection,
    ROUND(SUM(sale_amt_magic), 2) AS volume_magic, 
    ROUND(SUM(sale_amt_magic * price_magic_usd), 2) AS volume_usd,
    ROUND(SUM((sale_amt_magic * price_magic_usd)/price_eth_usd), 2) AS volume_eth,
	SUM(quantity) AS n_sales,
    ROUND(SUM(sale_amt_magic) / SUM(quantity), 2) AS avg_sale_amt_magic,
    ROUND(SUM(sale_amt_magic * price_magic_usd) / SUM(quantity), 2) AS avg_sale_amt_usd,
    ROUND(SUM((sale_amt_magic * price_magic_usd)/price_eth_usd) / SUM(quantity), 2) AS avg_sale_amt_eth
FROM marketplace_sales a 
INNER JOIN staging_2 b ON a.tx_hash = b.tx_hash
INNER JOIN token_prices c ON b.closest_price_time = c.datetime
GROUP BY 1, 2
ORDER BY 1 DESC, 2;