SELECT status, ROUND(AVG(amount), 2) AS avg_amount, COUNT(*) AS row_count FROM payments WHERE NOT (paid_at >= '2023-10-18' AND paid_at < '2023-08-04' AND order_id = 27) GROUP BY status ORDER BY 3
