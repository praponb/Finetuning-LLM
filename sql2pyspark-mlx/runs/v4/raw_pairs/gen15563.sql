SELECT id, store_id, SUBSTRING(status, 1, 3) AS status_prefix
FROM orders
WHERE customer_id IS NOT NULL
ORDER BY status_prefix, store_id;
