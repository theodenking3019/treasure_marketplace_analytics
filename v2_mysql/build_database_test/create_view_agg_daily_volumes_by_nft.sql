CREATE OR REPLACE VIEW agg_daily_vol_by_nft (
    date,
    nft_collection,
    nft,
    volume_magic,
    n_sales, 
    avg_sale_amt_magic,
    volume_usd,
    volume_eth,
    avg_sale_amt_usd,
    avg_sale_amt_eth
)
AS
WITH avg_daily_prices AS (
SELECT 
	DATE(datetime) AS date, 
    AVG(price_magic_usd) AS price_magic_usd, 
    AVG(price_eth_usd) AS price_eth_usd
FROM token_prices
GROUP BY 1),
daily_magic_volume AS (
SELECT 
	DATE(a.datetime) AS date, 
	nft_collection,
    nft_subcategory AS nft,
    ROUND(SUM(sale_amt_magic), 2) AS volume_magic, 
	SUM(quantity) AS n_sales,
    ROUND(SUM(sale_amt_magic) / SUM(quantity), 2) AS avg_sale_amt_magic
FROM marketplace_sales a 
GROUP BY 1, 2, 3
)
SELECT 
	a.*,
	volume_magic * price_magic_usd AS volume_usd,
    (volume_magic * price_magic_usd) / price_eth_usd AS volume_eth,
	(volume_magic * price_magic_usd) / n_sales AS avg_sale_amt_usd,
    ((volume_magic * price_magic_usd) / price_eth_usd) / n_sales AS avg_sale_amt_eth
FROM daily_magic_volume a
INNER JOIN avg_daily_prices b ON a.date = b.date;