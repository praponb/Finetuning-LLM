SELECT
    order_id,
    product_id,
    CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END AS quantity_bucket,
    COUNT(DISTINCT quantity) AS unique_products,
    MIN(unit_price) AS min_unit_price,
    MAX(unit_price) - MIN(unit_price) AS unit_price_range
FROM order_items
GROUP BY order_id, product_id, CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END
ORDER BY quantity_bucket
