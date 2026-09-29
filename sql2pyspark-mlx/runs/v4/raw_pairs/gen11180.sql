SELECT oi.id, oi.order_id, oi.quantity FROM order_items oi WHERE oi.product_id NOT IN (SELECT id FROM products WHERE ARRAY_CONTAINS(tags, 'premium')) ORDER BY order_id DESC
