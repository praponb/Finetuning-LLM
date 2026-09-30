SELECT p.id, p.category, COUNT(ss.id) AS store_sale_count FROM products p LEFT OUTER JOIN store_sales ss ON ss.product_id = p.id GROUP BY p.id, p.category ORDER BY category
