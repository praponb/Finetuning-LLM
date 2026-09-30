SELECT p.price, p.launch_date, ss.revenue, ss.sale_date FROM products p LEFT JOIN store_sales ss ON ss.product_id = p.id ORDER BY p.price DESC
