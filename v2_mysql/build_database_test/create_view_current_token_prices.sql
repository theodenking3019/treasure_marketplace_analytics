CREATE OR REPLACE VIEW treasure.current_token_prices AS (
SELECT 1 AS id, price_magic_usd, price_eth_usd
FROM treasure.token_prices
WHERE datetime = (SELECT MAX(datetime) FROM treasure.token_prices)
);