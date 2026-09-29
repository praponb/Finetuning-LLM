WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity, id) AS row_seq,
        NTILE(5) OVER (PARTITION BY store_id ORDER BY quantity DESC, id) AS quantity_bucket
    FROM inventory
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity_bucket DESC, id) = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        row_seq,
        quantity_bucket,
        PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY quantity_bucket NULLS LAST) AS quantity_bucket_percent_rank,
        ROUND(quantity_bucket / SUM(quantity_bucket) OVER (PARTITION BY store_id), 4) AS quantity_bucket_share,
        DENSE_RANK() OVER w_running AS row_seq_dense_rank,
        MIN(quantity) OVER w_hist AS store_id_min_quantity,
        AVG(quantity) OVER (PARTITION BY product_id, store_id ORDER BY row_seq, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS quantity_avg_5
    FROM latest
    WINDOW w_running AS (PARTITION BY store_id ORDER BY row_seq DESC), w_hist AS (PARTITION BY store_id)
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', last_restock) AS quarter_start,
    CASE WHEN quantity_bucket_share >= 0.89 THEN 'LARGE' ELSE 'SMALL' END AS quantity_bucket_share_tier,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    APPROX_PERCENTILE(quantity_avg_5, ARRAY(0.25, 0.5, 0.75)) AS quantity_avg_5_quartiles,
    COUNT(*) AS event_count,
    PERCENTILE_APPROX(row_seq, 0.95) AS row_seq_p95,
    ROUND(AVG(quantity), 2) AS avg_quantity_2,
    APPROX_PERCENTILE(quantity_avg_5, 0.9) AS quantity_avg_5_p90
FROM with_windows
GROUP BY DATE_TRUNC('QUARTER', last_restock), CASE WHEN quantity_bucket_share >= 0.89 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY quantity_avg_5_p90 DESC
