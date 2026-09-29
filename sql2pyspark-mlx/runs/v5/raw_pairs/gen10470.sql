SELECT
    id,
    order_id,
    product_id,
    unit_price,
    ROUND(quantity, 0) AS quantity_rounded,
    ROUND(unit_price * 1.15, 2) AS adjusted_unit_price
FROM order_items
WHERE unit_price < 210 AND quantity != 7
ORDER BY id
