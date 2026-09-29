SELECT ss.id, ss.revenue, s.sqft, p.stock_qty, p.brand FROM store_sales ss INNER JOIN stores s ON ss.store_id = s.id INNER JOIN products p ON ss.product_id = p.id
