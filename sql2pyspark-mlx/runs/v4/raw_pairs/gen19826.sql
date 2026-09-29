WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DAY(start_date) start_date_day,
        DATE_SUB(start_date, 83) start_date_minus_83d
    FROM subscriptions
    WHERE end_date > DATE '2024-05-30'
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id, plan ORDER BY start_date DESC NULLS FIRST, id ASC) = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY monthly_fee DESC NULLS LAST) monthly_fee_percent_rank,
        NTILE(10) OVER (PARTITION BY customer_id, status ORDER BY start_date_day DESC, id ASC) start_date_day_bucket,
        SUM(monthly_fee) OVER (PARTITION BY customer_id ORDER BY start_date_day ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_monthly_fee,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY plan), 4) monthly_fee_share
    FROM ranked
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM metrics
    QUALIFY RANK() OVER (PARTITION BY customer_id, plan ORDER BY start_date_day DESC) = 1
),
deduped_subscriptions AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY monthly_fee_share DESC NULLS FIRST, id ASC) AS dedup_rank
    FROM deduped
    QUALIFY dedup_rank = 1
)
SELECT
    id,
    customer_id,
    monthly_fee_percent_rank,
    start_date_day_bucket
FROM deduped_subscriptions
ORDER BY start_date_day_bucket DESC;
