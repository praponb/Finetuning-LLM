SELECT DATE_FORMAT(o.order_date, 'yyyy-MM') AS order_month, ROUND(STDDEV(oi.unit_price), 2) AS stddev_unit_price, MAX(oi.quantity) - MIN(oi.quantity) AS quantity_range
FROM order_items oi
JOIN orders o ON o.id = oi.order_id
WHERE o.channel IN ('store', 'web', 'mobile')
GROUP BY DATE_FORMAT(o.order_date, 'yyyy-MM')
ORDER BY quantity_range DESC, order_month
LIMIT 3;
