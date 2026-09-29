SELECT p.id, p.category, p.product_name, p.cost FROM products p WHERE EXISTS (SELECT 1 FROM store_sales ss WHERE ss.product_id = p.id AND ss.revenue <= 3500)
