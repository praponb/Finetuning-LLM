WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LOWER(plan) AS plan_lower,
        FLOOR(monthly_fee) AS monthly_fee_whole
    FROM subscriptions
    WHERE end_date <= '2024-02-10'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(monthly_fee_whole) OVER (PARTITION BY plan ORDER BY monthly_fee, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS monthly_fee_whole_avg_3,
        NTILE(10) OVER (PARTITION BY customer_id, plan ORDER BY monthly_fee_whole DESC, id) AS monthly_fee_whole_bucket,
        SUM(monthly_fee) OVER (PARTITION BY status ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_monthly_fee,
        NTILE(5) OVER (ORDER BY monthly_fee_whole, id) AS monthly_fee_whole_bucket_2
    FROM valid
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, monthly_fee_whole / NULLIF(monthly_fee_whole_avg_3, 0) AS monthly_fee_whole_per_monthly_fee_whole_avg_3
    FROM enriched
    WHERE start_date >= DATE '2023-09-11'
)
SELECT
    id,
    customer_id,
    end_date,
    monthly_fee,
    status,
    plan_lower,
    monthly_fee_whole_avg_3,
    monthly_fee_whole_bucket_2,
    CASE WHEN status RLIKE '^(EXPIRED|CANCELLED)$' THEN 'CORE' WHEN status RLIKE '^(ACTIVE|PAUSED)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS status_zone
FROM filtered;
