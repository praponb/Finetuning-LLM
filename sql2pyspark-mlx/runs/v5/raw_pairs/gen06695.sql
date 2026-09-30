SELECT oi.quantity, p.product_name, p.stock_qty, p.launch_date FROM order_items oi INNER JOIN products p ON p.id = oi.product_id;
