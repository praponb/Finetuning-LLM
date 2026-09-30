SELECT
    product_id,
    CASE WHEN quantity >= 222 THEN 'PREMIUM' ELSE 'STANDARD' END AS quantity_tier,
    APPROX_PERCENTILE(quantity, 0.95) AS quantity_p95,
    PERCENTILE_APPROX(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles,
    MIN(quantity) AS min_quantity,
    DENSE_RANK() OVER (PARTITION BY product_id ORDER BY APPROX_PERCENTILE(quantity, 0.95) DESC) AS quantity_p95_dense_rank
FROM inventory
GROUP BY 1, 2
HAVING APPROX_PERCENTILE(quantity, 0.95) < 0
