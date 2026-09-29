SELECT
    CASE WHEN unit_price >= 310 THEN 'HIGH' ELSE 'LOW' END AS unit_price_tier,
    product_id,
    APPROX_PERCENTILE(quantity, 0.5) AS quantity_p50,
    COUNT(DISTINCT id) AS distinct_ids
FROM order_items
GROUP BY CASE WHEN unit_price >= 310 THEN 'HIGH' ELSE 'LOW' END, product_id
ORDER BY distinct_ids;
