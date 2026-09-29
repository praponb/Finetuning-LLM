SELECT status, MIN(paid_at) AS min_paid_at FROM payments WHERE order_id IS NULL AND TO_DATE(paid_at) < '2023-04-20' AND order_id = 1 GROUP BY status ORDER BY status
