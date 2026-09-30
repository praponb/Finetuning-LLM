WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT(plan, ' ', status) AS plan_status
    FROM subscriptions
    WHERE plan IN ('enterprise', 'standard', 'basic')
)
-- Ranking and gap features
SELECT
    id,
    customer_id,
    start_date,
    end_date,
    monthly_fee,
    LAG(monthly_fee) OVER (PARTITION BY status ORDER BY monthly_fee, id) AS prev_monthly_fee,
    LEAD(monthly_fee) OVER (PARTITION BY status ORDER BY monthly_fee, id) AS next_monthly_fee,
    ROW_NUMBER() OVER (PARTITION BY status ORDER BY monthly_fee, id) AS seq_no,
    DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY monthly_fee DESC NULLS FIRST) AS monthly_fee_dense_rank
FROM base
