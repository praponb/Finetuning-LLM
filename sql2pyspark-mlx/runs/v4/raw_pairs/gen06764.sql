SELECT o.id, o.customer_id FROM orders o WHERE o.id IN (SELECT order_id FROM order_items WHERE product_id IN (1, 2, 20, 26)) ORDER BY o.id DESC;
