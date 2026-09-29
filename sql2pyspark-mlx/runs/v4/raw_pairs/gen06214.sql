WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(quantity, 2) AS quantity_rounded
    FROM order_items
    WHERE product_id IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        quantity_rounded,
        LAST_VALUE(quantity) IGNORE NULLS OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity,
        CUME_DIST() OVER (PARTITION BY product_id ORDER BY quantity_rounded DESC NULLS LAST) AS quantity_rounded_cume_dist,
        LAG(quantity, 1) OVER (PARTITION BY order_id, product_id ORDER BY quantity, id) AS prev_quantity
    FROM filtered
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY last_known_quantity DESC, id) = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, product_id
    FROM deduped
    EXCEPT ALL
    SELECT id, product_id
    FROM order_items
    WHERE order_id IN (8, 11, 17)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    JOIN remaining_keys k ON s.id = k.id AND s.product_id = k.product_id
),
filtered_order_items AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        quantity_rounded_cume_dist + quantity AS quantity_rounded_cume_dist_plus_quantity,
        ROUND(quantity_rounded_cume_dist / unit_price, 2) AS quantity_rounded_cume_dist_per_unit_price
    FROM kept_rows
    WHERE product_id IS NOT NULL
),
scored_order_items AS (
    -- Ranking and gap features
    SELECT
        *,
        STDDEV_SAMP(quantity_rounded_cume_dist) OVER (PARTITION BY order_id) AS order_id_stddev_samp_quantity_rounded_cume_dist,
        RANK() OVER (PARTITION BY order_id ORDER BY quantity_rounded DESC NULLS FIRST) AS quantity_rounded_rank,
        CUME_DIST() OVER (PARTITION BY order_id ORDER BY quantity_rounded_cume_dist DESC NULLS FIRST) AS quantity_rounded_cume_dist_cume_dist
    FROM filtered_order_items
),
with_dims AS (
    -- Attach products attributes
    SELECT src.*, b.brand
    FROM scored_order_items src
    LEFT JOIN products b
        ON src.product_id = b.id
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        order_id,
        brand,
        quantity
    FROM with_dims
) src
PIVOT (
    AVG(quantity) AS value_sum
    FOR brand IN ('Acme' AS acme, 'Umbrella' AS umbrella)
)
