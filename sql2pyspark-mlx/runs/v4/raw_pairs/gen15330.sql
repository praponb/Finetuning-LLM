WITH enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY order_id ORDER BY quantity DESC, id) AS quantity_bucket,
        CORR(quantity, unit_price) OVER (PARTITION BY order_id) AS corr_quantity_unit_price
    FROM order_items
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM enriched
    QUALIFY RANK() OVER (PARTITION BY order_id ORDER BY corr_quantity_unit_price DESC) = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(5) OVER (ORDER BY corr_quantity_unit_price, id) AS corr_quantity_unit_price_bucket,
        LEAD(quantity, 2, 0) OVER (PARTITION BY order_id ORDER BY unit_price, id) AS next_quantity,
        AVG(unit_price) OVER (PARTITION BY order_id) AS order_id_avg_unit_price,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price, id) AS seq_no
    FROM deduped
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(quantity_bucket, TRUE) OVER (PARTITION BY order_id ORDER BY order_id_avg_unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity_bucket,
        DENSE_RANK() OVER (PARTITION BY product_id ORDER BY next_quantity DESC NULLS LAST) AS next_quantity_dense_rank,
        ROUND(quantity_bucket / SUM(quantity_bucket) OVER (PARTITION BY product_id, order_id), 4) AS quantity_bucket_share,
        AVG(quantity_bucket) OVER (PARTITION BY order_id, product_id ORDER BY quantity, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS quantity_bucket_avg_5_prior
    FROM metrics
)
-- Multi-level aggregation
SELECT
    seq_no,
    CASE WHEN first_known_quantity_bucket >= 9 THEN 'HIGH' ELSE 'LOW' END AS first_known_quantity_bucket_tier,
    APPROX_PERCENTILE(order_id_avg_unit_price, 0.9) AS order_id_avg_unit_price_p90,
    BIT_OR(quantity) AS quantity_bit_or,
    BIT_AND(quantity) AS quantity_bit_and,
    COUNT(DISTINCT product_id) AS distinct_product_ids
FROM scored
GROUP BY seq_no, first_known_quantity_bucket_tier
ORDER BY distinct_product_ids DESC NULLS LAST;
