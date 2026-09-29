WITH cleaned AS (
    SELECT
        id,
        product_id,
        quantity,
        unit_price,
        CASE WHEN unit_price >= 410 THEN 'High' WHEN unit_price >= 190 THEN 'Medium' ELSE 'Low' END AS unit_price_tier,
        ROUND(unit_price * 0.9, 2) AS unit_price_adjusted
    FROM order_items
    WHERE product_id IS NOT NULL
),
remaining_keys AS (
    SELECT id, product_id
    FROM cleaned
    EXCEPT
    SELECT id, product_id
    FROM order_items
    WHERE unit_price BETWEEN 220 AND 390
),
kept_rows AS (
    SELECT s.*
    FROM cleaned s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.product_id = k.product_id
),
joined AS (
    SELECT
        s.*,
        p.category,
        p.price,
        p.cost
    FROM kept_rows s
    LEFT JOIN products p
        ON s.product_id = p.id
),
with_windows AS (
    SELECT
        id,
        product_id,
        quantity,
        unit_price,
        unit_price_tier,
        price,
        DENSE_RANK() OVER (PARTITION BY category ORDER BY cost DESC NULLS FIRST) AS cost_dense_rank,
        CUME_DIST() OVER (PARTITION BY product_id ORDER BY cost NULLS LAST) AS cost_cume_dist
    FROM joined
),
enriched AS (
    SELECT s.*, p.price AS product_price
    FROM with_windows s
    LEFT JOIN products p
        ON s.product_id = p.id
    WHERE p.category = 'Grocery'
)
SELECT
    product_id,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    COUNT(DISTINCT product_id) AS distinct_product_ids_2,
    ROUND(AVG(unit_price), 2) AS avg_unit_price,
    BIT_AND(quantity) AS quantity_bit_and,
    PERCENTILE_APPROX(cost_dense_rank, 0.5) AS cost_dense_rank_p50,
    COUNT(DISTINCT id) AS distinct_ids
FROM enriched
GROUP BY product_id
