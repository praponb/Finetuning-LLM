SELECT p.id, p.brand, p.launch_date FROM products p WHERE p.cost BETWEEN 320 AND 710 AND NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.product_id = p.id) ORDER BY brand;
