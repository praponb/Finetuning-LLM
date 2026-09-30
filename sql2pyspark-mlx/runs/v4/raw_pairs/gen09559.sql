SELECT oi.id, oi.order_id FROM order_items oi WHERE oi.product_id IN (SELECT id FROM products WHERE stock_qty BETWEEN 139 AND 232);
