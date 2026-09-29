SELECT oi.id, oi.unit_price, oi.product_id, oi.order_id FROM order_items oi WHERE oi.product_id IN (SELECT id FROM products WHERE stock_qty >= 245);
