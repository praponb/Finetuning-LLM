WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        status,
        LENGTH(plan) AS plan_length
    FROM subscriptions
    WHERE customer_id IS NOT NULL
      AND start_date > DATE '2023-03-17'
)
-- QUALIFY-style window filter
SELECT *, RANK() OVER (PARTITION BY customer_id, plan ORDER BY end_date DESC) AS rank_in_group
FROM base
QUALIFY rank_in_group = 1
