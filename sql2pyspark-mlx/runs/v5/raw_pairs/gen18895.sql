SELECT products.id, products.product_name FROM products WHERE EXISTS (SELECT 1 FROM store_sales WHERE store_sales.product_id = products.id AND store_sales.revenue BETWEEN 2400 AND 2500);
