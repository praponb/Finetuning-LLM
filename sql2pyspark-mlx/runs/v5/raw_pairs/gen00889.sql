WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        COALESCE(quantity, 0) AS quantity_filled,
        ROUND(unit_price / quantity, 2) AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
      AND product_id = 15
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        quantity,
        line_total,
        DENSE_RANK() OVER (PARTITION BY order_id ORDER BY line_total DESC NULLS LAST) AS line_total_dense_rank,
        DENSE_RANK() OVER (ORDER BY quantity_filled ASC) AS quantity_filled_dense_rank,
        NTILE(4) OVER (PARTITION BY order_id ORDER BY line_total DESC, id ASC) AS line_total_bucket,
        AVG(line_total) OVER (PARTITION BY order_id ORDER BY line_total ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS line_total_avg_5,
        COUNT(quantity_filled) OVER (PARTITION BY product_id, order_id) AS product_id_count_quantity_filled
    FROM base
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        line_total,
        line_total_dense_rank,
        quantity_filled_dense_rank,
        line_total_bucket,
        line_total_avg_5,
        product_id_count_quantity_filled,
        ROUND(product_id_count_quantity_filled * 1.1, 2) AS adjusted_product_id_count_quantity_filled
    FROM metrics
    WHERE line_total_avg_5 IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(line_total_dense_rank) OVER (ORDER BY quantity_filled_dense_rank ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS line_total_dense_rank_avg_3,
        LEAD(line_total_avg_5, 1) OVER (ORDER BY line_total ASC, id ASC) AS next_line_total_avg_5,
        NTILE(4) OVER (ORDER BY line_total_bucket DESC, id ASC) AS line_total_bucket_bucket
    FROM filtered
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        line_total,
        line_total_bucket,
        line_total_dense_rank_avg_3,
        next_line_total_avg_5,
        ROW_NUMBER() OVER w_part AS event_seq,
        RANK() OVER (ORDER BY line_total_bucket_bucket DESC NULLS LAST) AS line_total_bucket_bucket_rank,
        LAST_VALUE(line_total, TRUE) OVER (ORDER BY line_total_dense_rank ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_line_total
    FROM enriched
    WINDOW w_part AS (ORDER BY line_total_dense_rank DESC, id ASC)
),
base_order_items AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        line_total,
        line_total_bucket,
        next_line_total_avg_5,
        event_seq,
        line_total_bucket_bucket_rank,
        last_known_line_total,
        ROUND(last_known_line_total * 1.15, 2) AS last_known_line_total_scaled
    FROM with_windows
    WHERE next_line_total_avg_5 <> 38
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(line_total_bucket_bucket_rank, 0) AS line_total_bucket_bucket_rank_filled
    FROM base_order_items
    WHERE event_seq BETWEEN 4 AND 5
)
-- Grouped metrics
SELECT
    CASE WHEN last_known_line_total_scaled >= 61 THEN 'PREMIUM' ELSE 'STANDARD' END AS last_known_line_total_scaled_tier,
    CASE WHEN next_line_total_avg_5 >= 62 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_line_total_avg_5_tier,
    CASE WHEN line_total_bucket_bucket_rank >= 8 THEN 'HIGH' ELSE 'LOW' END AS line_total_bucket_bucket_rank_tier,
    PERCENTILE_APPROX(line_total_bucket_bucket_rank, ARRAY(0.25, 0.5, 0.75)) AS line_total_bucket_bucket_rank_quartiles,
    ROUND(AVG(last_known_line_total), 2) AS avg_last_known_line_total,
    COUNT(DISTINCT id) AS distinct_ids
FROM cleaned
GROUP BY CASE WHEN last_known_line_total_scaled >= 61 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN next_line_total_avg_5 >= 62 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN line_total_bucket_bucket_rank >= 8 THEN 'HIGH' ELSE 'LOW' END
HAVING avg_last_known_line_total >= 0
ORDER BY line_total_bucket_bucket_rank_tier DESC
