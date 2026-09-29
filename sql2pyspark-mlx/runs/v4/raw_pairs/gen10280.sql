SELECT s.city, COUNT(ss.id) AS store_sale_count FROM stores s LEFT JOIN store_sales ss ON ss.store_id = s.id GROUP BY s.city
