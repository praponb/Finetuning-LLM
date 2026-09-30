WITH base AS (
    SELECT *
    FROM payments
    WHERE method = 'bank_transfer' AND order_id IN (19, 35, 38)
),
payments_stats AS (
    SELECT method, status, ROUND(AVG(amount), 2) AS avg_amount
    FROM base
    GROUP BY method, status
)
SELECT *
FROM payments_stats
