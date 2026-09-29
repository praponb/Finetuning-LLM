SELECT oi.id, oi.unit_price, oi.order_id, oi.quantity, p.brand, p.product_name, p.launch_date FROM order_items oi INNER JOIN products p ON oi.product_id = p.id ORDER BY order_id DESC;
