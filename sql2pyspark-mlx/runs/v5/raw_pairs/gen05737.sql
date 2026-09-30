SELECT o.id, o.amount, o.status
FROM orders o
WHERE o.store_id IN (SELECT id FROM stores WHERE sqft > 4869)
ORDER BY o.status
