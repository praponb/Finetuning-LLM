SELECT DATE_FORMAT(s.opened_date, 'yyyy-MM') AS opened_month, AVG(oi.unit_price) AS avg_unit_price
FROM order_items oi
JOIN orders o ON o.id = oi.order_id
RIGHT JOIN stores s ON o.store_id = s.id
GROUP BY DATE_FORMAT(s.opened_date, 'yyyy-MM')
ORDER BY opened_month
