WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        start_date,
        end_date,
        monthly_fee,
        status,
        ROUND(monthly_fee * 0.85, 2) AS monthly_fee_scaled,
        TRIM(plan) AS clean_plan
    FROM subscriptions
    WHERE customer_id IS NOT NULL
      AND end_date < '2025-01-06'
)
-- Keep one row per key (latest first)
SELECT *
FROM (
    SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY monthly_fee_scaled) AS dedup_rank
    FROM base
) t
WHERE dedup_rank = 1
