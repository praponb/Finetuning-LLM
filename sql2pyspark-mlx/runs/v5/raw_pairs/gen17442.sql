SELECT
    product_id,
    COUNT(*) AS num_rows,
    APPROX_PERCENTILE(units, 0.5) AS units_p50,
    APPROX_PERCENTILE(units, ARRAY(0.25, 0.5, 0.75)) AS units_quartiles
FROM store_sales
GROUP BY ROLLUP (product_id)
