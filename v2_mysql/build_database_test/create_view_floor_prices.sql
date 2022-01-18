CREATE OR REPLACE VIEW floor_prices AS (
SELECT 
	nft_collection, 
    nft_subcategory, 
    MIN(listing_price_magic) AS floor_price
FROM treasure.marketplace_listings 
WHERE update_tx_hash IS NULL 
AND cancellation_tx_hash IS NULL
AND final_sale_tx_hash IS NULL
AND expires_at  > current_date()
GROUP BY 1, 2
);
