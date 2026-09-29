WITH valid AS (
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        monthly_fee,
        status,
        CEIL(monthly_fee) AS monthly_fee_whole,
        SUBSTRING(plan, 1, 2) AS plan_prefix
    FROM subscriptions
    WHERE end_date >= DATE '2024-03-07'
      AND customer_id IS NOT NULL
),
with_windows AS (
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        monthly_fee,
        status,
        monthly_fee_whole,
        plan_prefix,
        LEAD(monthly_fee_whole, 2, 0) OVER (ORDER BY start_date, id) AS next_monthly_fee_whole,
        SUM(monthly_fee_whole) OVER (PARTITION BY customer_id, status ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_monthly_fee_whole
    FROM valid
),
kept_keys AS (
    SELECT plan
    FROM with_windows
    EXCEPT ALL
    SELECT plan
    FROM subscriptions
    WHERE customer_id IS NOT NULL
),
reconciled AS (
    SELECT s.*
    FROM with_windows s
    INNER JOIN kept_keys k ON s.plan = k.plan
),
summary AS (
    SELECT
        status,
        DATE_TRUNC('QUARTER', start_date) AS quarter_start,
        SUM(CASE WHEN status = 'CANCELLED' THEN next_monthly_fee_whole ELSE 0 END) AS cancelled_next_monthly_fee_whole,
        AVG(monthly_fee_whole) AS avg_monthly_fee_whole,
        COUNT(DISTINCT customer_id) AS distinct_customer_ids,
        GROUPING_ID() AS grouping_level
    FROM reconciled
    GROUP BY CUBE (status, DATE_TRUNC('QUARTER', start_date))
)
SELECT
    status,
    quarter_start,
    cancelled_next_monthly_fee_whole,
    avg_monthly_fee_whole,
    distinct_customer_ids,
    grouping_level,
    CASE WHEN status RLIKE '^(EXPIRED|ACTIVE)$' THEN 'CORE' ELSE 'GROUP_B' END AS status_zone
FROM summary
ORDER BY avg_monthly_fee_whole NULLS LAST, quarter_start
