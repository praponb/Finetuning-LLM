SELECT
    DATE_TRUNC('YEAR', last_restock) AS year_start,
    store_id,
    PERCENTILE_APPROX(quantity, 0.9) AS quantity_p90,
    STDDEV_SAMP(quantity) AS quantity_stddev,
    MAX(quantity) AS max_quantity,
    MIN(quantity) AS min_quantity,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    ROUND(AVG(quantity), 2) AS avg_quantity_2,
    RANK() OVER (PARTITION BY DATE_TRUNC('YEAR', last_restock) ORDER BY MIN(quantity) DESC) AS min_quantity_rank
FROM inventory
GROUP BY 1, 2;
