SELECT o.id, o.status, o.store_id
FROM orders o
WHERE o.customer_id IN (SELECT id FROM customers WHERE NOT is_active)
ORDER BY id DESC
