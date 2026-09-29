SELECT ss.product_id, s.region, COALESCE(s.store_name, 'N/A') AS store_name_filled FROM store_sales ss INNER JOIN stores s ON s.id = ss.store_id ORDER BY product_id DESC;
