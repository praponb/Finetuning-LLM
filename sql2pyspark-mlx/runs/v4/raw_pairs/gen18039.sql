WITH kept_keys AS (
    SELECT order_id, id
    FROM order_items
    EXCEPT DISTINCT
    SELECT order_id, id
    FROM order_items
    WHERE product_id IS NOT NULL
),
reconciled AS (
    SELECT s.*
    FROM order_items s
    INNER JOIN kept_keys k ON s.order_id = k.order_id AND s.id = k.id
)
SELECT
    CASE WHEN unit_price >= 430 THEN 'HIGH' ELSE 'LOW' END AS unit_price_tier,
    APPROX_PERCENTILE(unit_price, ARRAY(0.25, 0.5, 0.75)) AS unit_price_quartiles,
    PERCENTILE_APPROX(unit_price, ARRAY(0.25, 0.5, 0.75)) AS unit_price_quartiles_2,
    COUNT(DISTINCT product_id) AS distinct_product_ids
FROM reconciled
GROUP BY CASE WHEN unit_price >= 430 THEN 'HIGH' ELSE 'LOW' END
