SELECT shipped_date
FROM shipments
WHERE cost BETWEEN 29 AND 33
EXCEPT
SELECT order_date
FROM orders
WHERE status = 'SHIPPED'
