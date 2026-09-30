SELECT ss.revenue, ss.sale_date, s.store_name, p.brand FROM store_sales ss INNER JOIN stores s ON ss.store_id = s.id INNER JOIN products p ON ss.product_id = p.id ORDER BY brand, store_name
