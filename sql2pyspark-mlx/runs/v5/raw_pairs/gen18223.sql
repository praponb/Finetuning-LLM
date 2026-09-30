SELECT order_id, product_id, min_unit_price
FROM (
    SELECT order_id, product_id, MIN(unit_price) AS min_unit_price
    FROM order_items
    GROUP BY order_id, product_id
) AS x
WHERE min_unit_price > 310
