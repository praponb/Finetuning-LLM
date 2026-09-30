WITH filtered AS (
    SELECT *, ROUND(monthly_fee, 0) AS monthly_fee_rounded
    FROM subscriptions
    WHERE customer_id IS NOT NULL
)
SELECT plan, status
FROM filtered
EXCEPT
SELECT plan, status
FROM subscriptions
WHERE end_date > '2025-05-08'
