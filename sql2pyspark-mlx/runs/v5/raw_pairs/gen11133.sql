WITH top_payments AS (SELECT id, method, paid_at, status, DENSE_RANK() OVER (PARTITION BY method ORDER BY paid_at DESC) pos FROM payments) SELECT * FROM top_payments WHERE pos = 1 ORDER BY 5, status
