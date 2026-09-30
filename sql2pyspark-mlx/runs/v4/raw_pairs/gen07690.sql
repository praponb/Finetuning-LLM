SELECT p.category, SUM(ss.revenue) AS total_revenue FROM store_sales ss INNER JOIN products p ON ss.product_id = p.id INNER JOIN stores s ON ss.store_id = s.id GROUP BY p.category;
