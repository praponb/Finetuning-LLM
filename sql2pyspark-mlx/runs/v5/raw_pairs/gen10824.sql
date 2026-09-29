SELECT cost
FROM shipments
WHERE carrier IS NOT NULL
INTERSECT
SELECT shipping_fee
FROM orders
WHERE status = 'PENDING'
ORDER BY cost DESC;
