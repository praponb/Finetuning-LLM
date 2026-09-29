SELECT
    CASE WHEN YEAR(orders.order_date) < 2024 THEN 'before_2024' WHEN YEAR(orders.order_date) = 2024 THEN '2024' ELSE 'after_2024' END AS order_cohort,
    orders.channel,
    MAX(order_items.quantity) AS max_quantity,
    SUM(order_items.quantity) AS total_quantity
FROM order_items
JOIN orders ON orders.id = order_items.order_id
JOIN products ON order_items.product_id = products.id
GROUP BY CASE WHEN YEAR(orders.order_date) < 2024 THEN 'before_2024' WHEN YEAR(orders.order_date) = 2024 THEN '2024' ELSE 'after_2024' END, orders.channel
ORDER BY orders.channel, 3;
