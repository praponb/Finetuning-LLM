SELECT
    CASE WHEN unit_price >= 320 THEN 'LARGE' ELSE 'SMALL' END AS unit_price_tier,
    APPROX_PERCENTILE(unit_price, ARRAY(0.25, 0.5, 0.75)) AS unit_price_quartiles,
    PERCENTILE_APPROX(unit_price, ARRAY(0.25, 0.5, 0.75)) AS unit_price_quartiles_2,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    AVG(quantity) AS avg_quantity_2,
    PERCENTILE_APPROX(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles
FROM order_items
GROUP BY CASE WHEN unit_price >= 320 THEN 'LARGE' ELSE 'SMALL' END
HAVING avg_quantity > 0
ORDER BY avg_quantity_2 DESC;
