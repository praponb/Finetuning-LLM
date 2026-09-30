SELECT o.channel, MAX(oi.quantity) - MIN(oi.quantity) AS quantity_range, COUNT(*) AS order_item_count
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
RIGHT OUTER JOIN products p ON oi.product_id = p.id
GROUP BY o.channel
HAVING MAX(oi.quantity) - MIN(oi.quantity) >= 47
ORDER BY channel DESC;
