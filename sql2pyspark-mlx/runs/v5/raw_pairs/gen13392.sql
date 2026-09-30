SELECT MONTH(p.launch_date) launch_month, MAX(oi.quantity) max_quantity, SUM(oi.unit_price * oi.quantity) revenue
FROM order_items oi
LEFT OUTER JOIN products p ON oi.product_id = p.id
GROUP BY MONTH(p.launch_date)
ORDER BY revenue DESC, max_quantity DESC;
