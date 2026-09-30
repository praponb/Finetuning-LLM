WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT(status, '/', plan) AS status_plan,
        TRIM(status) AS clean_status
    FROM subscriptions
    WHERE start_date > TO_DATE('2023-07-13')
      AND plan IN ('enterprise', 'basic')
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        monthly_fee,
        status
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY plan, status ORDER BY monthly_fee NULLS LAST, id) AS latest_rank
        FROM base
    ) ranked
    WHERE latest_rank = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(monthly_fee) OVER (PARTITION BY customer_id, plan ORDER BY start_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS monthly_fee_avg_6,
        NTILE(5) OVER (PARTITION BY status ORDER BY monthly_fee, id) AS monthly_fee_bucket,
        SUM(monthly_fee) OVER (PARTITION BY customer_id ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_monthly_fee
    FROM latest
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY plan ORDER BY monthly_fee DESC NULLS FIRST) AS dedup_rank
    FROM enriched
    QUALIFY dedup_rank = 1
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('WEEK', start_date) AS week_start,
    DATE_TRUNC('MONTH', start_date) AS activity_month,
    COUNT(CASE WHEN plan = 'basic' THEN 1 END) AS basic_count,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
    ROUND(AVG(monthly_fee_avg_6), 2) AS avg_monthly_fee_avg_6
FROM ranked
GROUP BY GROUPING SETS ((DATE_TRUNC('WEEK', start_date), DATE_TRUNC('MONTH', start_date)), (DATE_TRUNC('WEEK', start_date)))
HAVING COUNT(CASE WHEN plan = 'basic' THEN 1 END) < 1000
