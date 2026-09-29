SELECT customer_id
FROM orders
EXCEPT ALL
SELECT customer_id
FROM orders
WHERE channel IN ('store', 'partner', 'web');
