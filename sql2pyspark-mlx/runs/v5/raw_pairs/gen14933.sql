WITH ranked_payments AS (SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY paid_at) AS pos FROM payments) SELECT * FROM ranked_payments WHERE pos = 1 ORDER BY method
