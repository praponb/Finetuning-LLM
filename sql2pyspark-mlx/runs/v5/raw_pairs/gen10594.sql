SELECT
    *,
    MAX(amount) OVER (PARTITION BY method, order_id ORDER BY amount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_amount,
    MIN(amount) OVER (PARTITION BY method, order_id ORDER BY amount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_amount,
    AVG(amount) OVER (PARTITION BY method, order_id ORDER BY amount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_amount
FROM payments
