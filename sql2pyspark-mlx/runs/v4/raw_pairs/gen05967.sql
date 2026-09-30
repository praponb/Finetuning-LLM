SELECT ss.id, ss.store_id, s.store_name, p.product_name, p.brand FROM store_sales ss JOIN stores s ON ss.store_id = s.id JOIN products p ON ss.product_id = p.id;
