WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_id,
        quantity,
        last_restock,
        COALESCE(quantity, 0) AS quantity_filled
    FROM inventory
    WHERE last_restock >= DATE '2023-06-16'
      AND store_id = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        quantity,
        last_restock,
        quantity_filled,
        NTILE(10) OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS quantity_bucket,
        COUNT(quantity_filled) OVER (PARTITION BY product_id) AS product_id_count_quantity_filled,
        NTILE(3) OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS quantity_bucket_2
    FROM base
    WHERE quantity_filled BETWEEN 20 AND 43
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM metrics
    QUALIFY RANK() OVER (PARTITION BY product_id ORDER BY product_id_count_quantity_filled DESC) = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(quantity_bucket / SUM(quantity_bucket) OVER (PARTITION BY product_id), 4) AS quantity_bucket_share,
        LAST_VALUE(quantity_bucket) IGNORE NULLS OVER (PARTITION BY product_id ORDER BY quantity_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity_bucket,
        DENSE_RANK() OVER (ORDER BY quantity_bucket NULLS LAST) AS quantity_bucket_dense_rank
    FROM deduped
),
with_windows_inventory AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(product_id_count_quantity_filled, TRUE) OVER w_ordered AS first_known_product_id_count_quantity_filled,
        NTILE(5) OVER w_recent AS quantity_filled_bucket,
        LEAD(last_known_quantity_bucket, 1, 0) OVER (PARTITION BY product_id ORDER BY quantity, id) AS next_last_known_quantity_bucket
    FROM with_windows
    WINDOW w_ordered AS (ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY product_id ORDER BY quantity_filled DESC, id)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(next_last_known_quantity_bucket, 1) OVER (PARTITION BY product_id ORDER BY last_restock, id) AS next_next_last_known_quantity_bucket,
        AVG(quantity_filled) OVER (PARTITION BY product_id ORDER BY product_id_count_quantity_filled, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS quantity_filled_avg_6_prior,
        NTILE(3) OVER (PARTITION BY product_id ORDER BY first_known_product_id_count_quantity_filled DESC, id) AS first_known_product_id_count_quantity_filled_bucket
    FROM with_windows_inventory
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity_bucket_2 DESC NULLS FIRST, id) AS rank_in_group
        FROM enriched
    ) x
    WHERE rank_in_group = 1
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', last_restock) AS activity_month,
    SUM(product_id_count_quantity_filled) AS total_product_id_count_quantity_filled,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    PERCENTILE_APPROX(quantity_filled_bucket, 0.95) AS quantity_filled_bucket_p95,
    APPROX_PERCENTILE(quantity_bucket_2, ARRAY(0.25, 0.5, 0.75)) AS quantity_bucket_2_quartiles,
    ROUND(AVG(first_known_product_id_count_quantity_filled_bucket), 2) AS avg_first_known_product_id_count_quantity_filled_bucket,
    COUNT(DISTINCT id) AS distinct_ids
FROM latest
GROUP BY DATE_TRUNC('MONTH', last_restock);
