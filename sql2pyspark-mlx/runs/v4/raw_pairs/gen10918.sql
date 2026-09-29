SELECT
    order_id,
    CASE WHEN unit_price >= 320 THEN 'high' WHEN unit_price >= 160 THEN 'mid' ELSE 'low' END AS unit_price_bucket,
    SUM(unit_price) AS total_unit_price,
    MAX(quantity) AS max_quantity
FROM order_items
GROUP BY order_id, CASE WHEN unit_price >= 320 THEN 'high' WHEN unit_price >= 160 THEN 'mid' ELSE 'low' END
ORDER BY order_id DESC, unit_price_bucket
LIMIT 3;
