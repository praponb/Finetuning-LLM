SELECT
    id,
    quantity,
    unit_price,
    GREATEST(quantity, unit_price) AS greatest_quantity_unit_price
FROM order_items
ORDER BY greatest_quantity_unit_price DESC, id
LIMIT 50
