CREATE OR REPLACE VIEW agg_daily_vol_by_collection (
    date,
    nft_collection,
    volume_magic,
    n_sales, 
    avg_sale_amt_magic
)
AS 
SELECT 
	DATE(datetime) AS date,
    nft_collection,
    nft_subcategory AS nft,
    ROUND(SUM(sale_amt_magic), 2) AS volume_magic, 
	SUM(quantity) AS n_sales,
    ROUND(SUM(sale_amt_magic) / SUM(quantity), 2) AS avg_sale_amt_magic
FROM marketplace_sales
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 2, 3;