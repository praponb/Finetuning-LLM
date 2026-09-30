SELECT
    product_id,
    order_id,
    SUM(unit_price) AS total_unit_price,
    SUM(quantity * unit_price) AS revenue
FROM order_items
WHERE unit_price IS NOT NULL
GROUP BY product_id, order_id
HAVING revenue > 23;
