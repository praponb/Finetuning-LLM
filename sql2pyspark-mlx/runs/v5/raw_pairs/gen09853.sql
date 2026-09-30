SELECT product_id, order_id, quantity FROM order_items WHERE quantity IS NOT NULL AND unit_price > 130 ORDER BY quantity DESC, order_id DESC;
