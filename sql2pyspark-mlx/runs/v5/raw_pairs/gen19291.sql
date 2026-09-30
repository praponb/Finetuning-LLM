WITH with_window AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM subscriptions e
    JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.start_date >= sub.start_date
       AND e.start_date < sub.end_date + INTERVAL 14 DAYS
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER w_grp monthly_fee_bucket,
        LEAD(subscription_monthly_fee, 2) OVER w_part next_subscription_monthly_fee,
        NTILE(10) OVER w_running subscription_monthly_fee_bucket,
        AVG(subscription_monthly_fee) OVER (ORDER BY subscription_id, id, subscription_id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) subscription_monthly_fee_avg_2_prior
    FROM with_window
    WHERE YEAR(end_date) = 2025
    WINDOW w_grp AS (PARTITION BY plan ORDER BY monthly_fee DESC, id, subscription_id), w_part AS (PARTITION BY status ORDER BY subscription_monthly_fee, id, subscription_id), w_running AS (PARTITION BY plan ORDER BY subscription_monthly_fee DESC, id, subscription_id)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(subscription_id) OVER w_part prev_subscription_id,
        LAG(next_subscription_monthly_fee) OVER w_part prev_next_subscription_monthly_fee,
        ROW_NUMBER() OVER w_part row_seq
    FROM metrics
    WINDOW w_part AS (PARTITION BY status ORDER BY subscription_id, id, subscription_id)
),
matched_keys AS (
    -- Set difference on keys
    SELECT customer_id, status
    FROM enriched
    EXCEPT
    SELECT customer_id, status
    FROM subscriptions
    WHERE customer_id IN (18, 32, 34, 37)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN matched_keys k ON s.customer_id = k.customer_id AND s.status = k.status
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        start_date,
        status,
        subscription_id,
        subscription_monthly_fee_bucket,
        subscription_monthly_fee_avg_2_prior,
        row_seq,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY plan), 4) monthly_fee_share,
        ROUND(next_subscription_monthly_fee / SUM(next_subscription_monthly_fee) OVER (PARTITION BY status), 4) next_subscription_monthly_fee_share,
        ROUND(subscription_monthly_fee_bucket / SUM(subscription_monthly_fee_bucket) OVER (PARTITION BY plan), 4) subscription_monthly_fee_bucket_share
    FROM reconciled
)
-- Multi-level aggregation
SELECT
    status,
    COUNT(*) AS event_count,
    COUNT(*) AS row_count,
    APPROX_PERCENTILE(subscription_monthly_fee_bucket, ARRAY(0.25, 0.5, 0.75)) AS subscription_monthly_fee_bucket_quartiles
FROM with_windows
GROUP BY 1
ORDER BY event_count DESC NULLS LAST;
