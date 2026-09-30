SELECT p.cost, p.launch_date, p.supplier_id, oi.unit_price, oi.quantity, oi.product_id FROM products p LEFT JOIN order_items oi ON oi.product_id = p.id ORDER BY launch_date;
