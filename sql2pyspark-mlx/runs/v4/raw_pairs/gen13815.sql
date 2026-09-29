SELECT
    order_id,
    CASE WHEN unit_price >= 370 THEN 'high' WHEN unit_price >= 90 THEN 'mid' ELSE 'low' END AS unit_price_bucket,
    CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END AS quantity_bucket,
    COUNT(*) AS num_order_items
FROM order_items
GROUP BY 1, 2, 3
HAVING COUNT(*) >= 4
ORDER BY num_order_items DESC, unit_price_bucket DESC;
