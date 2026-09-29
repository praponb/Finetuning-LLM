SELECT oi.id, oi.unit_price, oi.quantity FROM order_items oi WHERE oi.product_id NOT IN (SELECT id FROM products WHERE supplier_id IN (5, 22, 24, 34));
