SELECT p.id, p.launch_date FROM products p WHERE p.category = 'Grocery' AND p.id IN (SELECT product_id FROM store_sales WHERE units IS NOT NULL)
