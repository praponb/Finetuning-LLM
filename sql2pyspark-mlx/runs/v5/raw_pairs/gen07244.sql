SELECT ss.id, ss.product_id, p.brand, s.city FROM store_sales ss INNER JOIN products p ON p.id = ss.product_id RIGHT JOIN stores s ON ss.store_id = s.id ORDER BY product_id DESC;
