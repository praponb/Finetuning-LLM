SELECT
    DATE_TRUNC('YEAR', last_restock) AS year_start,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(quantity, 0.9) AS quantity_p90
FROM inventory
GROUP BY DATE_TRUNC('YEAR', last_restock)
HAVING quantity_p90 < 0;
