WITH enriched AS (
    -- Attach products attributes
    SELECT src.*, d.product_name
    FROM order_items src
    LEFT JOIN products d
        ON src.product_id = d.id
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY quantity, id) AS rn
    FROM enriched
    QUALIFY rn = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        product_name,
        rn,
        LAG(quantity, 2) OVER (PARTITION BY order_id ORDER BY quantity, id) AS prev_quantity,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY rn DESC, id) AS seq_no
    FROM latest
),
with_windows_order_items AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(unit_price / SUM(unit_price) OVER (PARTITION BY product_id), 4) AS unit_price_share,
        AVG(quantity) OVER (PARTITION BY product_id) AS product_id_avg_quantity
    FROM with_windows
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN unit_price_share >= 0.64 THEN 'PREMIUM' ELSE 'STANDARD' END AS unit_price_share_tier,
    product_id,
    BIT_AND(quantity) AS quantity_bit_and,
    BIT_AND(quantity) AS quantity_bit_and_2,
    MAX(product_id_avg_quantity) AS max_product_id_avg_quantity,
    ROUND(AVG(seq_no), 2) AS avg_seq_no,
    APPROX_PERCENTILE(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles,
    PERCENTILE_APPROX(unit_price_share, 0.9) AS unit_price_share_p90
FROM with_windows_order_items
GROUP BY CUBE (CASE WHEN unit_price_share >= 0.64 THEN 'PREMIUM' ELSE 'STANDARD' END, product_id)
HAVING quantity_bit_and < 1;
