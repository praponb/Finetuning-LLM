WITH filtered AS (
    SELECT
        id,
        product_id,
        ROUND(quantity, 2) AS quantity_rounded
    FROM order_items
    WHERE product_id IS NOT NULL
      AND unit_price <= 400
),
with_windows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY quantity_rounded, id) AS seq_no,
        NTILE(3) OVER (PARTITION BY product_id ORDER BY quantity_rounded DESC, id) AS quantity_rounded_bucket,
        SUM(quantity_rounded) OVER (PARTITION BY product_id ORDER BY quantity_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity_rounded,
        RANK() OVER (ORDER BY quantity_rounded DESC NULLS FIRST) AS quantity_rounded_rank,
        SUM(quantity_rounded) OVER (ORDER BY quantity_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity_rounded_2
    FROM filtered
    WHERE product_id IN (4, 21, 32, 38)
),
enriched AS (
    SELECT
        *,
        AVG(seq_no) OVER w_part AS seq_no_avg_5,
        NTILE(3) OVER (ORDER BY seq_no DESC, id) AS seq_no_bucket,
        LAST_VALUE(seq_no) IGNORE NULLS OVER (ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_seq_no,
        SUM(running_quantity_rounded_2) OVER (PARTITION BY product_id ORDER BY running_quantity_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_quantity_rounded_2,
        ROW_NUMBER() OVER w_grp AS seq_no_2
    FROM with_windows
    WINDOW w_part AS (PARTITION BY product_id ORDER BY seq_no, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY product_id ORDER BY seq_no DESC, id)
),
filtered_order_items AS (
    SELECT
        id,
        quantity_rounded,
        seq_no,
        quantity_rounded_bucket,
        running_quantity_rounded,
        quantity_rounded_rank,
        running_quantity_rounded_2,
        seq_no_avg_5,
        seq_no_bucket,
        last_known_seq_no,
        seq_no_2,
        ROUND(seq_no_bucket * 0.85, 2) AS seq_no_bucket_adjusted,
        CASE WHEN quantity_rounded_rank >= 8 THEN 'High' WHEN quantity_rounded_rank >= 3 THEN 'Medium' ELSE 'Low' END AS quantity_rounded_rank_level
    FROM enriched
    WHERE product_id IS NOT NULL
),
metrics AS (
    SELECT
        *,
        AVG(seq_no_bucket) OVER w_recent AS last3_avg_seq_no_bucket,
        MIN(seq_no_bucket) OVER w_recent AS last3_min_seq_no_bucket,
        COUNT(seq_no_bucket) OVER w_recent AS last3_count_seq_no_bucket
    FROM filtered_order_items
    WINDOW w_recent AS (ORDER BY seq_no_avg_5, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
)
SELECT
    CASE WHEN quantity_rounded_bucket >= 8 THEN 'HIGH' ELSE 'LOW' END AS quantity_rounded_bucket_tier,
    CASE WHEN quantity_rounded >= 79 THEN 'LARGE' ELSE 'SMALL' END AS quantity_rounded_tier,
    CASE WHEN seq_no_avg_5 >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS seq_no_avg_5_tier,
    PERCENTILE_APPROX(last3_count_seq_no_bucket, 0.5) AS last3_count_seq_no_bucket_p50,
    COUNT(DISTINCT id) AS distinct_ids,
    MIN(running_quantity_rounded_2) AS min_running_quantity_rounded_2,
    COUNT(DISTINCT id) AS distinct_ids_2,
    COUNT(DISTINCT id) AS distinct_ids_3
FROM metrics
GROUP BY quantity_rounded_bucket_tier, quantity_rounded_tier, seq_no_avg_5_tier
HAVING MIN(running_quantity_rounded_2) < 1000000
ORDER BY seq_no_avg_5_tier NULLS FIRST
