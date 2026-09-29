WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        TRIM(plan) AS clean_plan
    FROM subscriptions
    WHERE start_date > TO_DATE('2023-05-22')
      AND end_date BETWEEN '2024-01-06' AND '2024-07-01'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        monthly_fee,
        clean_plan,
        DENSE_RANK() OVER w_recent AS monthly_fee_dense_rank,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY plan, status), 4) AS monthly_fee_share,
        CUME_DIST() OVER (ORDER BY monthly_fee DESC NULLS LAST) AS monthly_fee_cume_dist
    FROM valid
    WINDOW w_recent AS (PARTITION BY status ORDER BY monthly_fee DESC NULLS LAST)
),
scored_subscriptions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(monthly_fee) OVER (PARTITION BY plan ORDER BY monthly_fee_share, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_monthly_fee,
        MIN(monthly_fee) OVER (PARTITION BY plan ORDER BY monthly_fee_share, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_monthly_fee,
        COUNT(monthly_fee) OVER (PARTITION BY plan ORDER BY monthly_fee_share, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY plan ORDER BY monthly_fee_dense_rank DESC, id) AS seq_no
    FROM scored
    WHERE monthly_fee_cume_dist >= 0.31
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        monthly_fee_cume_dist,
        last4_max_monthly_fee,
        last4_min_monthly_fee
    FROM scored_subscriptions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY plan ORDER BY monthly_fee_cume_dist DESC NULLS LAST, id) = 1
),
scored_subscriptions_2 AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        last4_max_monthly_fee,
        COUNT(monthly_fee_cume_dist) OVER w_running AS last4_count_monthly_fee_cume_dist,
        MAX(monthly_fee_cume_dist) OVER w_running AS last4_max_monthly_fee_cume_dist,
        AVG(monthly_fee_cume_dist) OVER w_running AS last4_avg_monthly_fee_cume_dist,
        NTILE(10) OVER w_ordered AS last4_min_monthly_fee_bucket
    FROM deduped
    WINDOW w_running AS (ORDER BY last4_max_monthly_fee, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), w_ordered AS (ORDER BY last4_min_monthly_fee, id)
)
-- Grouped metrics
SELECT
    CASE WHEN last4_min_monthly_fee_bucket >= 7 THEN 'LARGE' ELSE 'SMALL' END AS last4_min_monthly_fee_bucket_tier,
    CASE WHEN last4_max_monthly_fee >= 193 THEN 'HIGH' ELSE 'LOW' END AS last4_max_monthly_fee_tier,
    ROUND(AVG(last4_avg_monthly_fee_cume_dist), 2) AS avg_last4_avg_monthly_fee_cume_dist,
    COUNT(*) AS row_count,
    APPROX_PERCENTILE(last4_max_monthly_fee_cume_dist, 0.95) AS last4_max_monthly_fee_cume_dist_p95
FROM scored_subscriptions_2
GROUP BY CASE WHEN last4_min_monthly_fee_bucket >= 7 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN last4_max_monthly_fee >= 193 THEN 'HIGH' ELSE 'LOW' END
