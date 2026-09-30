SELECT
    product_id,
    order_id,
    COUNT(quantity) AS quantity_count,
    MAX(unit_price) AS max_unit_price
FROM order_items
WHERE quantity BETWEEN 6 AND 8 OR unit_price BETWEEN 140 AND 250
GROUP BY product_id, order_id
HAVING COUNT(unit_price) < 2
