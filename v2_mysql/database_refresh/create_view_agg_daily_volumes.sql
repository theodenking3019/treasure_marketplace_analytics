CREATE OR REPLACE VIEW agg_daily_volume (
    date,
    volume_magic,
    n_sales, 
    avg_sale_amt_magic
)
AS 
SELECT 
	DATE(datetime) AS date, 
    ROUND(SUM(sale_amt_magic), 2) AS volume_magic, 
	SUM(quantity) AS n_sales,
    ROUND(SUM(sale_amt_magic) / SUM(quantity), 2) AS avg_sale_amt_magic
FROM marketplace_sales
GROUP BY 1
ORDER BY 1 DESC;