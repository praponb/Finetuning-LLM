WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LAST_DAY(end_date) AS end_date_month_end,
        DATE_TRUNC('YEAR', start_date) AS start_date_period
    FROM subscriptions
    WHERE end_date > DATE '2024-03-25'
      AND customer_id IS NOT NULL
      AND status = 'CANCELLED'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        end_date_month_end,
        start_date_period,
        SUM(monthly_fee) OVER w_running AS monthly_fee_sum_7d,
        NTILE(3) OVER (PARTITION BY status ORDER BY monthly_fee DESC, id) AS monthly_fee_bucket,
        SUM(monthly_fee) OVER w_recent AS running_monthly_fee
    FROM valid
    WINDOW w_running AS (PARTITION BY plan, status ORDER BY CAST(start_date_period AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY customer_id ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        plan,
        end_date,
        monthly_fee,
        status,
        end_date_month_end,
        monthly_fee_bucket,
        CASE WHEN plan IN ('enterprise', 'standard') THEN 'Core' ELSE 'Other' END AS plan_group
    FROM metrics
    WHERE end_date >= TO_DATE('2024-04-17')
      AND customer_id IS NOT NULL
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY monthly_fee DESC, id) AS latest_rank
        FROM base
    ) t
    WHERE latest_rank = 1
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', end_date_month_end) AS year_start,
    DATE_TRUNC('QUARTER', end_date_month_end) AS quarter_start,
    AVG(monthly_fee) FILTER (WHERE status = 'PAUSED') AS paused_avg_monthly_fee,
    APPROX_PERCENTILE(monthly_fee_bucket, 0.9) AS monthly_fee_bucket_p90,
    GROUPING_ID() AS gid
FROM ranked
GROUP BY ROLLUP (DATE_TRUNC('YEAR', end_date_month_end), DATE_TRUNC('QUARTER', end_date_month_end))
ORDER BY monthly_fee_bucket_p90 DESC;
