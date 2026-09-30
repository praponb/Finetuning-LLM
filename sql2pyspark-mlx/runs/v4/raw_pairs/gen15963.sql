SELECT
    id,
    quantity,
    unit_price,
    unit_price + quantity AS line_total,
    quantity + unit_price AS line_total_2
FROM order_items
WHERE unit_price IS NOT NULL;
