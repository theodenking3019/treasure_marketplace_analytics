# marketplace_listings table tests

# Test 1: counts of updates, cancellations, and sales seem reasonable
SELECT 
	SUM(1) AS count_listings,
	SUM(CASE WHEN update_tx_hash IS NOT NULL THEN 1 ELSE 0 END) AS count_updates,
    SUM(CASE WHEN cancellation_tx_hash IS NOT NULL THEN 1 ELSE 0 END) AS count_cancellations,
    SUM(CASE WHEN final_sale_tx_hash IS NOT NULL THEN 1 ELSE 0 END) AS count_sales
FROM
	treasure_test.marketplace_listings;
    
# Test 2: no tx_hashes are duplicated
SELECT tx_hash, COUNT(1) 
FROM treasure_test.marketplace_listings
GROUP BY 1
HAVING COUNT(1) > 1;

SELECT update_tx_hash, COUNT(1) 
FROM treasure_test.marketplace_listings
WHERE update_tx_hash IS NOT NULL
GROUP BY 1
HAVING COUNT(1) > 1;

SELECT cancellation_tx_hash, COUNT(1) 
FROM treasure_test.marketplace_listings
WHERE cancellation_tx_hash IS NOT NULL
GROUP BY 1
HAVING COUNT(1) > 1;

SELECT final_sale_tx_hash, COUNT(1) 
FROM treasure_test.marketplace_listings
WHERE final_sale_tx_hash IS NOT NULL
GROUP BY 1
HAVING COUNT(1) > 1;

# Test 2: no cases where tx_hash equals update_tx_hash
SELECT *
FROM treasure_test.marketplace_listings
WHERE tx_hash = update_tx_hash;

# Test 3: all listings amounts are the same as sale amounts
SELECT a.*, ROUND(a.listing_price_magic, 2), (b.sale_amt_magic / b.quantity), b.quantity
FROM treasure_test.marketplace_listings a 
INNER JOIN marketplace_sales b ON a.final_sale_tx_hash = b.tx_hash
WHERE ROUND(a.listing_price_magic, 2) <> ROUND((b.sale_amt_magic / b.quantity),2);

# Test 4: there are no cases where two transition states are not null
SELECT * 
FROM treasure_test.marketplace_listings
WHERE (update_tx_hash IS NOT NULL AND cancellation_tx_hash IS NOT NULL)
OR (update_tx_hash IS NOT NULL AND final_sale_tx_hash IS NOT NULL)
OR (cancellation_tx_hash IS NOT NULL AND final_sale_tx_hash IS NOT NULL);

SELECT * 
FROM treasure_test.marketplace_listings
WHERE update_tx_hash = '0xa00c7360ba3ca891cf51af5917a916ee6c52546de0a7c2fcad25bab54de5c946'; 