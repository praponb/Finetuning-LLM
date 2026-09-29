SELECT id, order_id, SUBSTRING(method, 1, 3) AS method_prefix FROM payments WHERE paid_at < '2023-06-22' AND method IS NOT NULL
