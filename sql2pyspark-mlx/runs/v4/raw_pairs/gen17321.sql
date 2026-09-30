SELECT order_date
FROM orders
WHERE channel = 'web'
EXCEPT
SELECT opened_date
FROM stores
ORDER BY order_date DESC;
