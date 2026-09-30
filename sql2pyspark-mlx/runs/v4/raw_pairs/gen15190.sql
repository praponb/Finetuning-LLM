WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        quantity + unit_price line_total
    FROM order_items
    WHERE product_id IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY product_id, order_id ORDER BY line_total DESC, id) line_total_bucket,
        LEAD(line_total, 2) OVER (PARTITION BY product_id ORDER BY unit_price, id) next_line_total,
        NTILE(10) OVER w_hist line_total_bucket_2,
        ROW_NUMBER() OVER (ORDER BY quantity DESC, id) seq_no,
        AVG(quantity) OVER w_ordered order_id_avg_quantity
    FROM filtered
    WINDOW w_hist AS (PARTITION BY order_id ORDER BY line_total, id), w_ordered AS (PARTITION BY order_id)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, seq_no * order_id_avg_quantity seq_no_times_order_id_avg_quantity
    FROM with_windows
    WHERE quantity BETWEEN 5 AND 8
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_id_avg_quantity NULLS LAST, id) = 1
),
stats AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN seq_no >= 9 THEN 'LARGE' ELSE 'SMALL' END AS seq_no_tier,
        product_id,
        MIN(next_line_total) AS min_next_line_total,
        PERCENTILE_APPROX(line_total_bucket_2, ARRAY(0.25, 0.5, 0.75)) AS line_total_bucket_2_quartiles
    FROM ranked
    GROUP BY CASE WHEN seq_no >= 9 THEN 'LARGE' ELSE 'SMALL' END, product_id
    HAVING min_next_line_total < 0
)
SELECT
    seq_no_tier,
    product_id,
    min_next_line_total,
    line_total_bucket_2_quartiles
FROM stats
ORDER BY min_next_line_total DESC NULLS FIRST
