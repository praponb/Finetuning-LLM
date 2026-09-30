WITH filtered_payments AS (
    SELECT *, COALESCE(method, 'Unknown') AS method_filled
    FROM payments
    WHERE order_id IN (16, 17, 36) AND TO_DATE(paid_at) < '2024-03-10'
),
method_summary AS (
    SELECT
        method,
        status,
        AVG(amount) AS avg_amount,
        MIN(amount) AS min_amount
    FROM filtered_payments
    GROUP BY method, status
)
SELECT method, avg_amount, min_amount
FROM method_summary
ORDER BY avg_amount
