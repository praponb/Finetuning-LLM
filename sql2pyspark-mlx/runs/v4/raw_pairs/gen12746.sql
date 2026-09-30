SELECT
    DATE_TRUNC('WEEK', last_restock) AS week_start,
    COUNT(*) AS num_rows,
    APPROX_PERCENTILE(quantity, 0.95) AS quantity_p95,
    APPROX_PERCENTILE(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    PERCENTILE_APPROX(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles_2,
    BIT_AND(quantity) AS quantity_bit_and
FROM inventory
GROUP BY DATE_TRUNC('WEEK', last_restock)
ORDER BY num_rows DESC;
