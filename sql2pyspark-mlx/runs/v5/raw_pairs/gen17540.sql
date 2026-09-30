WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        shipped_date,
        delivered_date,
        cost,
        ROUND(cost / SUM(cost) OVER (PARTITION BY carrier), 4) AS cost_share,
        NTILE(10) OVER (PARTITION BY status, order_id ORDER BY cost, id) AS cost_bucket
    FROM shipments
    WHERE cost <= 67
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY delivered_date DESC, id) AS rn
        FROM with_windows
    ) t
    WHERE rn = 1
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, DATEDIFF(CURRENT_DATE(), shipped_date) AS days_since_shipped_date
    FROM ranked
    WHERE shipped_date > DATE '2023-03-16'
      AND cost > 26
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        shipped_date,
        cost_share,
        cost_bucket,
        days_since_shipped_date,
        PERCENT_RANK() OVER (PARTITION BY order_id ORDER BY cost_bucket DESC) AS cost_bucket_percent_rank,
        LAST_VALUE(cost) IGNORE NULLS OVER w_recent AS last_known_cost,
        AVG(cost_bucket) OVER (PARTITION BY order_id ORDER BY delivered_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS cost_bucket_avg_2_prior
    FROM cleaned
    WINDOW w_recent AS (PARTITION BY order_id ORDER BY cost_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics_shipments AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(days_since_shipped_date) OVER w_part AS running_sum_days_since_shipped_date,
        COUNT(days_since_shipped_date) OVER w_part AS running_count_days_since_shipped_date,
        AVG(days_since_shipped_date) OVER w_part AS running_avg_days_since_shipped_date,
        MIN(days_since_shipped_date) OVER w_part AS running_min_days_since_shipped_date,
        PERCENT_RANK() OVER (PARTITION BY order_id ORDER BY cost_bucket_percent_rank DESC NULLS FIRST) AS cost_bucket_percent_rank_percent_rank
    FROM metrics
    WINDOW w_part AS (PARTITION BY order_id ORDER BY last_known_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Join a dimension and aggregate
SELECT
    p.channel,
    p.status,
    COUNT(*) AS cnt,
    ROUND(AVG(e.cost_bucket_avg_2_prior), 2) AS avg_cost_bucket_avg_2_prior,
    ROUND(AVG(e.running_count_days_since_shipped_date), 2) AS avg_running_count_days_since_shipped_date
FROM metrics_shipments e
LEFT JOIN orders p ON e.order_id = p.id
GROUP BY p.channel, p.status
ORDER BY status
