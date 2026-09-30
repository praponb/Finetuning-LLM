WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_id,
        quantity,
        unit_price,
        ROUND(quantity, 1) AS quantity_rounded
    FROM order_items
    WHERE product_id IS NOT NULL
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id, product_id
    FROM filtered
    INTERSECT ALL
    SELECT id, product_id
    FROM order_items
    WHERE quantity > 6
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    JOIN kept_keys k ON s.id = k.id AND s.product_id = k.product_id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        quantity,
        unit_price,
        quantity_rounded,
        DENSE_RANK() OVER (PARTITION BY product_id ORDER BY quantity DESC NULLS LAST) AS quantity_dense_rank,
        MAX(unit_price) OVER w_grp AS product_id_max_unit_price,
        COUNT(quantity_rounded) OVER w_grp AS product_id_count_quantity_rounded,
        CORR(quantity, unit_price) OVER w_grp AS corr_quantity_unit_price,
        CUME_DIST() OVER (PARTITION BY product_id ORDER BY unit_price DESC) AS unit_price_cume_dist
    FROM verified
    WINDOW w_grp AS (PARTITION BY product_id)
),
enriched AS (
    -- Attach products attributes
    SELECT a.*, d.stock_qty
    FROM scored a
    LEFT JOIN products d
        ON a.product_id = d.id
)
-- Multi-level aggregation
SELECT
    CASE WHEN corr_quantity_unit_price >= 0.26 THEN 'LARGE' ELSE 'SMALL' END AS corr_quantity_unit_price_tier,
    CASE WHEN quantity_rounded >= 68 THEN 'LARGE' ELSE 'SMALL' END AS quantity_rounded_tier,
    product_id,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    BIT_AND(quantity) AS quantity_bit_and,
    APPROX_PERCENTILE(product_id_max_unit_price, 0.95) AS product_id_max_unit_price_p95,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN corr_quantity_unit_price >= 0.26 THEN 'LARGE' ELSE 'SMALL' END ORDER BY COUNT(DISTINCT product_id) DESC) AS distinct_product_ids_dense_rank
FROM enriched
GROUP BY CASE WHEN corr_quantity_unit_price >= 0.26 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN quantity_rounded >= 68 THEN 'LARGE' ELSE 'SMALL' END, product_id
