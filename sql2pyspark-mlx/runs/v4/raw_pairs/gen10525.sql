SELECT
    id,
    product_id,
    unit_price - quantity AS line_total,
    CEIL(quantity) AS quantity_whole
FROM order_items
WHERE unit_price BETWEEN 320 AND 400 OR product_id IN (3, 9, 15, 40)
ORDER BY quantity_whole, id
LIMIT 5;
