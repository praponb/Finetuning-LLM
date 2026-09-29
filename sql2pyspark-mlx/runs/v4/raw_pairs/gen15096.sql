WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        plan,
        monthly_fee,
        LENGTH(plan) AS plan_length
    FROM subscriptions
    WHERE start_date >= TO_DATE('2023-08-25')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        plan,
        monthly_fee,
        plan_length,
        COUNT(plan_length) OVER (PARTITION BY customer_id) AS customer_id_count_plan_length,
        NTILE(10) OVER (PARTITION BY plan ORDER BY monthly_fee, id) AS monthly_fee_bucket,
        ROW_NUMBER() OVER (ORDER BY monthly_fee DESC, id) AS row_seq
    FROM cleaned
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY plan_length DESC, id) AS latest_rank
        FROM metrics
    ) x
    WHERE latest_rank = 1
),
stats AS (
    -- Grouped metrics
    SELECT
        plan,
        SIZE(COLLECT_SET(plan)) AS num_plans,
        PERCENTILE_APPROX(monthly_fee, 0.95) AS monthly_fee_p95,
        COUNT(*) FILTER (WHERE plan = 'enterprise') AS enterprise_rows
    FROM latest
    GROUP BY plan
)
SELECT plan, monthly_fee_p95
FROM stats;
