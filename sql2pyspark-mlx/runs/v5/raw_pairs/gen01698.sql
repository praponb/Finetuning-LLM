SELECT
    order_id,
    unit_price,
    GREATEST(quantity, unit_price) greatest_quantity_unit_price,
    ROUND(unit_price / quantity, 2) line_total
FROM order_items
WHERE unit_price < 380
ORDER BY order_id DESC;
