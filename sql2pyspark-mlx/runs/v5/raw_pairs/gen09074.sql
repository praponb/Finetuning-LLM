WITH ranked_payments AS (SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY amount DESC) AS rn FROM payments) SELECT * FROM ranked_payments WHERE rn = 1;
