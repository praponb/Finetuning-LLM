SELECT
    unit_price,
    product_id,
    quantity,
    LEAST(quantity, unit_price) AS least_quantity_unit_price,
    COALESCE(unit_price, 0) AS unit_price_filled
FROM order_items
WHERE quantity = 7 OR product_id = 15
ORDER BY least_quantity_unit_price DESC;
