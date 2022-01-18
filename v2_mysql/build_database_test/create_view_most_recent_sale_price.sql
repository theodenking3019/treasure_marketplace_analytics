CREATE OR REPLACE VIEW treasure.most_recent_sale_prices AS (
WITH most_recent_sales AS (
SELECT 
	nft_collection, 
    nft_subcategory, 
    MAX(datetime) AS most_recent_sale_datetime
FROM treasure.marketplace_sales
GROUP BY 1, 2
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY sale_amt_magic) id, 
	a.nft_collection, 
    a.nft_subcategory,
	a.sale_amt_magic / a.quantity AS most_recent_sale_price_magic
FROM treasure.marketplace_sales a
INNER JOIN most_recent_sales b ON a.nft_collection = b.nft_collection AND a.nft_subcategory = b.nft_subcategory AND a.datetime = b.most_recent_sale_datetime
);

