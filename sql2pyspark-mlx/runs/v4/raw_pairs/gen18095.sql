SELECT
    id,
    product_id,
    quantity,
    GREATEST(quantity, unit_price) greatest_quantity_unit_price
FROM order_items
WHERE NOT (unit_price BETWEEN 200 AND 220 AND unit_price BETWEEN 310 AND 350);
