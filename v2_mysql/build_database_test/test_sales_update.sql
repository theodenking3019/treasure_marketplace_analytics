WITH test AS (
SELECT * FROM treasure_test.marketplace_sales WHERE datetime > CAST('2021-12-26 11:54:02' AS DATETIME)),
prod AS (SELECT * FROM treasure.marketplace_sales WHERE datetime > CAST('2021-12-26 11:54:02' AS DATETIME))
SELECT * FROM test a INNER JOIN prod b ON 
a.tx_hash = b.tx_hash AND
a.datetime = b.datetime AND 
a.wallet_buyer = b.wallet_buyer AND
a.wallet_seller = b.wallet_seller AND 
a.sale_amt_magic = b.sale_amt_magic AND
a.seller_amt_received_magic = b.seller_amt_received_magic AND
a.dao_amt_received_magic = b.dao_amt_received_magic AND
a.gas_fee_eth = b.gas_fee_eth AND
a.nft_collection = b.nft_collection AND
a.nft_id = b.nft_id AND
a.quantity = b.quantity AND 
(a.nft_name = b.nft_name  OR (a.nft_name IS NULL AND b.nft_name IS NULL)) AND 
(a.nft_subcategory = b.nft_subcategory  OR (a.nft_subcategory IS NULL AND b.nft_subcategory IS NULL));