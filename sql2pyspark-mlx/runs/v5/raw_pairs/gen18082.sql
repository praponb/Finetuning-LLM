SELECT
    id,
    order_id,
    product_id,
    unit_price,
    quantity + unit_price AS line_total,
    quantity * unit_price AS line_total_2
FROM order_items
WHERE unit_price BETWEEN 130 AND 370 AND order_id IN (20, 27);
