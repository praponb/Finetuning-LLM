SELECT
    DATE_TRUNC('YEAR', last_restock) AS year_start,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    PERCENTILE_APPROX(quantity, 0.95) AS quantity_p95,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT product_id) AS distinct_product_ids_2,
    STDDEV_SAMP(quantity) AS quantity_stddev
FROM inventory
GROUP BY DATE_TRUNC('YEAR', last_restock)
ORDER BY distinct_product_ids NULLS FIRST
