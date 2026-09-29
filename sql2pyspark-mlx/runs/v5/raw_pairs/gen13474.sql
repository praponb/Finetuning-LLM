SELECT *, LENGTH(method) AS method_length FROM payments WHERE order_id IN (11, 12, 21, 30) AND paid_at < '2024-06-01' ORDER BY id DESC LIMIT 10
