SELECT *, ROUND(amount * 100, 2) AS amount_adjusted
FROM orders
WHERE amount <= 920
ORDER BY order_date DESC, id
LIMIT 20;
