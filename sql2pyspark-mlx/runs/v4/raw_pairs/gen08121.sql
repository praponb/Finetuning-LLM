SELECT *
FROM orders
WHERE channel IN ('web', 'store', 'partner') AND (channel IN ('partner', 'web', 'store') OR customer_id IN (5, 16, 35))
ORDER BY status;
