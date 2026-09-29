SELECT p.id, p.cost, p.category, p.launch_date FROM products p WHERE NOT EXISTS (SELECT 1 FROM store_sales ss WHERE ss.product_id = p.id) ORDER BY p.id DESC, p.category;
