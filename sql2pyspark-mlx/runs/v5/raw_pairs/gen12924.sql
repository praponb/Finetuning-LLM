SELECT order_id, min_quantity
FROM (
    SELECT order_id, MIN(quantity) min_quantity
    FROM order_items
    GROUP BY order_id
) s
WHERE min_quantity < 5.9
ORDER BY min_quantity DESC, order_id DESC
LIMIT 10
