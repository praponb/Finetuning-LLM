SELECT cost
FROM shipments
WHERE status <> 'DELIVERED'
INTERSECT
SELECT shipping_fee AS cost
FROM orders
ORDER BY cost;
