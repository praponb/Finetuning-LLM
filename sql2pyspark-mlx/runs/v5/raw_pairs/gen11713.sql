SELECT order_items.id, order_items.unit_price, order_items.product_id FROM order_items WHERE order_items.product_id NOT IN (SELECT id FROM products WHERE cost BETWEEN 450 AND 670) ORDER BY id
