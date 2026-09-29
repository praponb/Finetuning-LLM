WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        CONCAT_WS('-', status, plan) AS status_plan_key
    FROM subscriptions
    WHERE customer_id IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        start_date,
        end_date,
        monthly_fee,
        status,
        NTILE(5) OVER w_grp AS monthly_fee_bucket,
        NTILE(10) OVER (PARTITION BY plan ORDER BY monthly_fee, id) AS monthly_fee_bucket_2,
        NTILE(4) OVER (ORDER BY monthly_fee DESC, id) AS monthly_fee_bucket_3,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY status, plan), 4) AS monthly_fee_share,
        CUME_DIST() OVER (PARTITION BY status ORDER BY monthly_fee DESC) AS monthly_fee_cume_dist
    FROM cleaned
    WINDOW w_grp AS (PARTITION BY status ORDER BY monthly_fee DESC, id)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY monthly_fee_bucket_3 NULLS FIRST, id) AS rank_in_group
        FROM enriched
    ) ranked
    WHERE rank_in_group = 1
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id, status
    FROM deduped
    INTERSECT
    SELECT id, status
    FROM subscriptions
    WHERE monthly_fee IS NOT NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    JOIN remaining_keys k ON s.id = k.id AND s.status = k.status
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('MONTH', start_date) AS activity_month,
    SUM(CASE WHEN status = 'EXPIRED' THEN monthly_fee_bucket_3 ELSE 0 END) AS expired_monthly_fee_bucket_3,
    ROUND(SUM(monthly_fee_bucket_2) / NULLIF(COUNT(CASE WHEN status = 'PAUSED' THEN 1 END), 0), 2) AS monthly_fee_bucket_2_per_paused,
    ROUND(AVG(monthly_fee_cume_dist), 2) AS avg_monthly_fee_cume_dist
FROM verified
GROUP BY DATE_TRUNC('MONTH', start_date)
