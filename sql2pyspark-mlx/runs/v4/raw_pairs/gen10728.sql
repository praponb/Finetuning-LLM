SELECT
    order_id,
    CASE WHEN quantity >= 9 THEN 'HIGH' ELSE 'LOW' END AS quantity_tier,
    product_id,
    COUNT(*) AS cnt,
    STDDEV_SAMP(unit_price) AS unit_price_stddev,
    PERCENTILE_APPROX(quantity, 0.9) AS quantity_p90,
    STDDEV_SAMP(unit_price) AS unit_price_stddev_2,
    BIT_AND(quantity) AS quantity_bit_and,
    GROUPING_ID() AS grouping_level
FROM order_items
GROUP BY GROUPING SETS ((order_id, CASE WHEN quantity >= 9 THEN 'HIGH' ELSE 'LOW' END, product_id), (order_id, CASE WHEN quantity >= 9 THEN 'HIGH' ELSE 'LOW' END), (order_id))
ORDER BY unit_price_stddev_2 NULLS LAST
