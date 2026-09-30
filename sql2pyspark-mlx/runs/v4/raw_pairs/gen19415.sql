SELECT p.id, p.launch_date, p.brand
FROM products p
WHERE ARRAY_CONTAINS(p.tags, 'bestseller') AND EXISTS (SELECT 1 FROM order_items oi WHERE oi.product_id = p.id AND oi.order_id = 15)
