WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        ROUND(monthly_fee * 1.15, 2) AS monthly_fee_adjusted
    FROM subscriptions
    WHERE start_date > DATE '2023-05-04'
      AND customer_id IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        plan,
        end_date,
        monthly_fee,
        status,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY status), 4) AS monthly_fee_share,
        MIN(monthly_fee) OVER (PARTITION BY customer_id, status) AS customer_id_min_monthly_fee
    FROM base
    WHERE monthly_fee BETWEEN 70 AND 160
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY customer_id_min_monthly_fee DESC, id) AS row_num
    FROM metrics
    QUALIFY row_num = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(customer_id_min_monthly_fee) OVER (PARTITION BY status ORDER BY row_num, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_customer_id_min_monthly_fee,
        NTILE(10) OVER (PARTITION BY plan ORDER BY monthly_fee_share DESC, id) AS monthly_fee_share_bucket,
        FIRST_VALUE(monthly_fee_share, TRUE) OVER (PARTITION BY customer_id ORDER BY monthly_fee_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_monthly_fee_share
    FROM deduped
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(monthly_fee_share_bucket, 2) AS monthly_fee_share_bucket_rounded,
        ROUND(row_num, 2) AS row_num_rounded
    FROM enriched
    WHERE end_date > DATE '2023-07-31'
      AND monthly_fee < 230
),
stats AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN running_customer_id_min_monthly_fee >= 1310 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_customer_id_min_monthly_fee_tier,
        PERCENTILE_APPROX(row_num_rounded, ARRAY(0.25, 0.5, 0.75)) AS row_num_rounded_quartiles,
        MIN(first_known_monthly_fee_share) AS min_first_known_monthly_fee_share,
        COUNT(CASE WHEN status = 'ACTIVE' THEN 1 END) AS active_count,
        COUNT_IF(plan = 'standard') AS standard_events,
        GROUPING(CASE WHEN running_customer_id_min_monthly_fee >= 1310 THEN 'PREMIUM' ELSE 'STANDARD' END) AS grouping_level
    FROM valid
    GROUP BY ROLLUP (CASE WHEN running_customer_id_min_monthly_fee >= 1310 THEN 'PREMIUM' ELSE 'STANDARD' END)
    HAVING MIN(first_known_monthly_fee_share) < 1000000
)
SELECT
    running_customer_id_min_monthly_fee_tier,
    row_num_rounded_quartiles,
    min_first_known_monthly_fee_share,
    standard_events,
    grouping_level
FROM stats
ORDER BY standard_events DESC NULLS LAST, grouping_level DESC;
