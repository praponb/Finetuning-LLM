SELECT
    unit_price_bucket,
    order_id,
    quantity_range,
    total_unit_price
FROM (
    SELECT
        CASE WHEN unit_price >= 420 THEN 'high' WHEN unit_price >= 60 THEN 'mid' ELSE 'low' END AS unit_price_bucket,
        order_id,
        MAX(quantity) - MIN(quantity) AS quantity_range,
        SUM(unit_price) AS total_unit_price
    FROM order_items
    WHERE order_id = 19
    GROUP BY CASE WHEN unit_price >= 420 THEN 'high' WHEN unit_price >= 60 THEN 'mid' ELSE 'low' END, order_id
) AS x
WHERE quantity_range < 70
ORDER BY total_unit_price DESC, order_id, unit_price_bucket
LIMIT 5
