SELECT YEAR(o.order_date) AS order_year, o.status, AVG(oi.quantity) AS avg_quantity
FROM order_items oi
RIGHT JOIN orders o ON oi.order_id = o.id
GROUP BY YEAR(o.order_date), o.status
ORDER BY order_year DESC
