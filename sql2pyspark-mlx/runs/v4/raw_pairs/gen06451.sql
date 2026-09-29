SELECT MONTH(p.launch_date) AS launch_month, COUNT(*) AS order_item_count
FROM order_items oi
LEFT OUTER JOIN orders o ON oi.order_id = o.id
INNER JOIN products p ON oi.product_id = p.id
WHERE SIZE(p.tags) > 1
GROUP BY MONTH(p.launch_date)
ORDER BY order_item_count;
