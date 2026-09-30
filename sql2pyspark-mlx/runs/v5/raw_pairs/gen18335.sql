SELECT o.channel, o.status, MAX(oi.quantity) AS max_quantity
FROM order_items oi
JOIN orders o ON oi.order_id = o.id
WHERE oi.unit_price >= 210
GROUP BY o.channel, o.status
ORDER BY o.status, 1
