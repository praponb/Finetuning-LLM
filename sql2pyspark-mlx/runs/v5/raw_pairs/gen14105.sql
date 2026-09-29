SELECT
    id,
    amount,
    status,
    SUM(amount) OVER (PARTITION BY order_id ORDER BY amount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_amount,
    MIN(amount) OVER (PARTITION BY order_id ORDER BY amount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_amount,
    COUNT(amount) OVER (PARTITION BY order_id ORDER BY amount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_amount,
    MAX(amount) OVER (PARTITION BY order_id ORDER BY amount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_amount
FROM payments
WHERE status = 'SUCCESS'
