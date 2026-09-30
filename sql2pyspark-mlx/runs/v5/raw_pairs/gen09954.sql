SELECT DISTINCT channel
FROM orders
WHERE amount < 1600
ORDER BY channel
LIMIT 5
