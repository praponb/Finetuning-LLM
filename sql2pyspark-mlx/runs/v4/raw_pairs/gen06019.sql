SELECT
    order_id,
    CASE WHEN quantity >= 8 THEN 'HIGH' ELSE 'LOW' END AS quantity_tier,
    APPROX_PERCENTILE(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles,
    APPROX_PERCENTILE(unit_price, 0.9) AS unit_price_p90,
    ROUND(AVG(unit_price), 2) AS avg_unit_price,
    APPROX_PERCENTILE(unit_price, ARRAY(0.25, 0.5, 0.75)) AS unit_price_quartiles,
    BIT_OR(quantity) AS quantity_bit_or,
    STDDEV_SAMP(quantity) AS quantity_stddev,
    GROUPING(order_id) AS gid
FROM order_items
GROUP BY GROUPING SETS ((order_id, CASE WHEN quantity >= 8 THEN 'HIGH' ELSE 'LOW' END), (order_id))
HAVING avg_unit_price >= 0
ORDER BY order_id DESC
