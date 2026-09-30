WITH metrics AS (
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        NTILE(4) OVER (PARTITION BY order_id ORDER BY quantity, id) AS quantity_bucket,
        LAST_VALUE(quantity, TRUE) OVER (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity
    FROM order_items
),
enriched AS (
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        quantity_bucket,
        last_known_quantity,
        DENSE_RANK() OVER (PARTITION BY product_id, order_id ORDER BY quantity_bucket DESC NULLS LAST) AS quantity_bucket_dense_rank,
        LAST_VALUE(last_known_quantity) IGNORE NULLS OVER (ORDER BY quantity_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_last_known_quantity,
        SUM(quantity_bucket) OVER w_part AS running_quantity_bucket,
        SUM(quantity) OVER (PARTITION BY order_id, product_id) AS order_id_sum_quantity,
        CORR(quantity, unit_price) OVER w_running AS corr_quantity_unit_price
    FROM metrics
    WINDOW w_part AS (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY order_id)
),
enriched_order_items AS (
    SELECT
        *,
        MAX(last_known_quantity) OVER (PARTITION BY order_id) AS order_id_max_last_known_quantity,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS seq_no,
        MIN(order_id_sum_quantity) OVER (PARTITION BY order_id) AS order_id_min_order_id_sum_quantity,
        NTILE(4) OVER (PARTITION BY order_id ORDER BY order_id_sum_quantity, id) AS order_id_sum_quantity_bucket,
        COUNT(quantity_bucket_dense_rank) OVER (PARTITION BY product_id, order_id) AS product_id_count_quantity_bucket_dense_rank
    FROM enriched
),
filtered AS (
    SELECT *, CAST(order_id_sum_quantity AS STRING) AS order_id_sum_quantity_num
    FROM enriched_order_items
    WHERE product_id IS NOT NULL
),
grouped AS (
    SELECT
        product_id,
        CASE WHEN product_id_count_quantity_bucket_dense_rank >= 13 THEN 'PREMIUM' ELSE 'STANDARD' END AS product_id_count_quantity_bucket_dense_rank_tier,
        CASE WHEN quantity_bucket_dense_rank >= 9 THEN 'HIGH' ELSE 'LOW' END AS quantity_bucket_dense_rank_tier,
        PERCENTILE_APPROX(last_known_last_known_quantity, ARRAY(0.25, 0.5, 0.75)) AS last_known_last_known_quantity_quartiles,
        BIT_AND(quantity) AS quantity_bit_and,
        GROUPING_ID() AS gid
    FROM filtered
    GROUP BY CUBE (product_id, CASE WHEN product_id_count_quantity_bucket_dense_rank >= 13 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN quantity_bucket_dense_rank >= 9 THEN 'HIGH' ELSE 'LOW' END)
    HAVING quantity_bit_and > 1
)
SELECT
    product_id,
    product_id_count_quantity_bucket_dense_rank_tier,
    quantity_bucket_dense_rank_tier,
    last_known_last_known_quantity_quartiles,
    quantity_bit_and,
    gid
FROM grouped
ORDER BY quantity_bit_and, gid
