SELECT
    id,
    product_id,
    unit_price,
    AVG(unit_price) OVER (PARTITION BY product_id) AS product_id_avg_unit_price
FROM order_items
WHERE unit_price >= 330
ORDER BY product_id;
