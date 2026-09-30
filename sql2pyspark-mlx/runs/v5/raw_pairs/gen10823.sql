WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        quantity,
        last_restock,
        ROUND(quantity * 100, 2) AS quantity_adjusted
    FROM inventory
    WHERE last_restock >= DATE '2023-09-01'
      AND store_id IN (2, 4)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        quantity_adjusted,
        SUM(quantity) OVER (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity,
        AVG(quantity_adjusted) OVER (PARTITION BY store_id ORDER BY quantity_adjusted, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS quantity_adjusted_avg_6_prior
    FROM base
),
with_windows_inventory AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(quantity_adjusted) OVER w_part AS prev_quantity_adjusted,
        LEAD(quantity_adjusted) OVER w_part AS next_quantity_adjusted,
        LEAD(running_quantity) OVER w_part AS next_running_quantity,
        LAG(running_quantity) OVER w_part AS prev_running_quantity,
        ROW_NUMBER() OVER w_part AS row_seq,
        LEAD(quantity_adjusted_avg_6_prior, 2) OVER w_part AS next_quantity_adjusted_avg_6_prior
    FROM with_windows
    WHERE quantity_adjusted IS NOT NULL
    WINDOW w_part AS (ORDER BY quantity_adjusted, id)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(next_quantity_adjusted) OVER w_recent AS next_quantity_adjusted_avg_2,
        NTILE(3) OVER (ORDER BY prev_running_quantity DESC, id) AS prev_running_quantity_bucket,
        ROW_NUMBER() OVER (ORDER BY next_quantity_adjusted_avg_6_prior DESC, id) AS event_seq,
        AVG(quantity_adjusted) OVER (ORDER BY row_seq, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS quantity_adjusted_avg_6,
        AVG(quantity_adjusted_avg_6_prior) OVER (ORDER BY quantity_adjusted_avg_6_prior, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS quantity_adjusted_avg_6_prior_avg_2
    FROM with_windows_inventory
    WINDOW w_recent AS (ORDER BY next_quantity_adjusted_avg_6_prior, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
grouped AS (
    -- Grouped metrics
    SELECT
        CASE WHEN event_seq >= 8 THEN 'HIGH' ELSE 'LOW' END AS event_seq_tier,
        CASE WHEN quantity_adjusted >= 88 THEN 'HIGH' ELSE 'LOW' END AS quantity_adjusted_tier,
        APPROX_PERCENTILE(prev_running_quantity_bucket, ARRAY(0.25, 0.5, 0.75)) AS prev_running_quantity_bucket_quartiles,
        COUNT(DISTINCT id) AS distinct_ids,
        GROUPING_ID() AS gid
    FROM enriched
    GROUP BY ROLLUP (CASE WHEN event_seq >= 8 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN quantity_adjusted >= 88 THEN 'HIGH' ELSE 'LOW' END)
)
SELECT
    event_seq_tier,
    quantity_adjusted_tier,
    distinct_ids,
    gid
FROM grouped
ORDER BY distinct_ids DESC NULLS LAST, event_seq_tier;
