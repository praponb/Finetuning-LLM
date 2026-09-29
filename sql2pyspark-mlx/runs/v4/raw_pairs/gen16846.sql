SELECT
    id,
    unit_price,
    product_id,
    ROUND(quantity, 0) quantity_rounded
FROM order_items
WHERE quantity < 4 AND order_id = 9 AND quantity >= 6
ORDER BY 2 DESC, id
LIMIT 10
