SELECT
    order_id,
    product_id,
    SUM(quantity * unit_price) AS revenue,
    ROUND(AVG(unit_price), 2) AS avg_unit_price
FROM order_items
GROUP BY order_id, product_id;
