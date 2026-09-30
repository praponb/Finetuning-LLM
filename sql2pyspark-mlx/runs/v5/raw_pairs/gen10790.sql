WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        CONCAT_WS('-', status, plan) AS status_plan_key
    FROM subscriptions
    WHERE start_date BETWEEN '2024-08-18' AND '2025-02-01'
)
-- Running and moving aggregates
SELECT
    *,
    COUNT(monthly_fee) OVER (PARTITION BY plan, customer_id ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_monthly_fee,
    MIN(monthly_fee) OVER (PARTITION BY plan, customer_id ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_monthly_fee,
    MAX(monthly_fee) OVER (PARTITION BY plan, customer_id ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_monthly_fee,
    SUM(monthly_fee) OVER (PARTITION BY plan, customer_id ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_monthly_fee
FROM valid
