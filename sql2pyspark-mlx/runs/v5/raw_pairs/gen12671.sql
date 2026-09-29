SELECT CASE WHEN YEAR(orders.order_date) < 2024 THEN 'before_2024' WHEN YEAR(orders.order_date) = 2024 THEN '2024' ELSE 'after_2024' END AS order_cohort, MAX(order_items.unit_price) - MIN(order_items.unit_price) AS unit_price_range
FROM order_items
RIGHT OUTER JOIN orders ON order_items.order_id = orders.id
GROUP BY CASE WHEN YEAR(orders.order_date) < 2024 THEN 'before_2024' WHEN YEAR(orders.order_date) = 2024 THEN '2024' ELSE 'after_2024' END
ORDER BY 2 DESC, order_cohort
LIMIT 1
