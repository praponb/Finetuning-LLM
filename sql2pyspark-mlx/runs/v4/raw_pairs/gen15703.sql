SELECT
    method,
    MONTH(paid_at) AS paid_month,
    COUNT(amount) AS amount_count,
    COUNT(DISTINCT amount) AS unique_orders
FROM payments
WHERE amount BETWEEN 960 AND 970
GROUP BY method, paid_month
HAVING COUNT(DISTINCT amount) >= 4
