SELECT opened_date
FROM accounts
INTERSECT
SELECT delivered_date
FROM shipments
WHERE order_id IN (5, 13, 26, 33);
