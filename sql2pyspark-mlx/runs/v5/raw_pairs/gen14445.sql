SELECT id, shipping_fee
FROM orders
WHERE amount > 1280 AND channel IS NULL
ORDER BY id DESC
LIMIT 50;
