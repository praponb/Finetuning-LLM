SELECT
    id,
    quantity,
    unit_price,
    LEAD(unit_price, 1) OVER (ORDER BY quantity, id) AS next_unit_price
FROM order_items
WHERE product_id IN (9, 18);
