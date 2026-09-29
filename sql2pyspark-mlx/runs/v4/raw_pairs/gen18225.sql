SELECT
    o.customer_id,
    o.discount,
    t.revenue,
    t.unique_products
FROM orders o
LEFT JOIN (SELECT order_id, SUM(quantity * unit_price) AS revenue, COUNT(DISTINCT unit_price) AS unique_products
    FROM order_items
    GROUP BY order_id) t ON o.id = t.order_id
ORDER BY discount DESC;
