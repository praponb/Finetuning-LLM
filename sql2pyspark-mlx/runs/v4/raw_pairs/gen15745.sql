SELECT order_items.id, order_items.product_id, order_items.order_id FROM order_items WHERE order_items.product_id IN (SELECT id FROM products WHERE stock_qty >= 321);
