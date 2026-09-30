SELECT DISTINCT channel, status
FROM orders
WHERE channel IS NULL
