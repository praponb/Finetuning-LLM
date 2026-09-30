WITH base AS (
    SELECT *
    FROM subscriptions
    WHERE plan = 'premium' OR status IN ('CANCELLED', 'EXPIRED')
),
CustomerIdSummary AS (
    SELECT customer_id, COUNT(CASE WHEN status = 'CANCELLED' THEN 1 END) AS cancelled_count, COUNT(monthly_fee) AS monthly_fee_count
    FROM base
    GROUP BY customer_id
)
SELECT customer_id, cancelled_count, monthly_fee_count
FROM CustomerIdSummary
WHERE cancelled_count < 5
ORDER BY customer_id DESC
