WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, WEEKOFYEAR(end_date) end_date_weekofyear
    FROM subscriptions
    WHERE plan IN ('premium', 'basic')
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(monthly_fee, TRUE) OVER (PARTITION BY status ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_monthly_fee,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY monthly_fee NULLS FIRST) monthly_fee_percent_rank,
        NTILE(5) OVER (PARTITION BY status ORDER BY end_date_weekofyear DESC, id) end_date_weekofyear_bucket
    FROM cleaned
),
joined AS (
    -- Attach customers attributes
    SELECT src.*, b.signup_date
    FROM metrics src
    LEFT JOIN customers b
        ON src.customer_id = b.id
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, plan
    FROM joined
    EXCEPT ALL
    SELECT id, plan
    FROM subscriptions
    WHERE YEAR(end_date) >= 2025
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    INNER JOIN matched_keys k ON s.id = k.id AND s.plan = k.plan
)
-- Aggregate with conditional metrics
SELECT
    plan,
    SIZE(COLLECT_SET(plan)) AS num_plans,
    COUNT(CASE WHEN status = 'ACTIVE' THEN 1 END) AS active_count,
    MAX(monthly_fee) FILTER (WHERE status = 'PAUSED') AS paused_max_monthly_fee,
    SUM(CASE WHEN plan = 'basic' THEN monthly_fee ELSE 0 END) AS basic_monthly_fee,
    GROUPING(plan) AS grouping_level
FROM verified
GROUP BY CUBE (plan)
ORDER BY paused_max_monthly_fee DESC NULLS FIRST;
