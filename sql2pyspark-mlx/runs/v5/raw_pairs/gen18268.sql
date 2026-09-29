WITH kept_keys AS (
    SELECT store_id, id
    FROM inventory
    INTERSECT
    SELECT store_id, id
    FROM inventory
    WHERE product_id IN (1, 9)
),
kept_rows AS (
    SELECT s.*
    FROM inventory s
    JOIN kept_keys k ON s.store_id = k.store_id AND s.id = k.id
)
SELECT
    product_id,
    DATE_TRUNC('YEAR', last_restock) AS year_start,
    APPROX_PERCENTILE(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles,
    COUNT(DISTINCT id) AS distinct_ids,
    MIN(quantity) AS min_quantity,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    APPROX_PERCENTILE(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles_2
FROM kept_rows
GROUP BY ROLLUP (product_id, DATE_TRUNC('YEAR', last_restock))
