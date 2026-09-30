WITH matched_keys AS (
    SELECT store_id
    FROM inventory
    INTERSECT ALL
    SELECT store_id
    FROM inventory
    WHERE product_id IN (1, 6, 8, 9)
),
verified AS (
    SELECT s.*
    FROM inventory s
    INNER JOIN matched_keys k ON s.store_id = k.store_id
)
SELECT
    store_id,
    CASE WHEN quantity >= 247 THEN 'PREMIUM' ELSE 'STANDARD' END AS quantity_tier,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    STDDEV_SAMP(quantity) AS quantity_stddev,
    MAX(quantity) AS max_quantity,
    PERCENTILE_APPROX(quantity, 0.5) AS quantity_p50,
    PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY PERCENTILE_APPROX(quantity, 0.5) DESC) AS quantity_p50_percent_rank
FROM verified
GROUP BY store_id, CASE WHEN quantity >= 247 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING ROUND(AVG(quantity), 2) >= 0
ORDER BY store_id DESC;
