SELECT products.id, products.category FROM products WHERE NOT EXISTS (SELECT 1 FROM store_sales WHERE store_sales.product_id = products.id AND store_sales.revenue BETWEEN 2500 AND 3300);
