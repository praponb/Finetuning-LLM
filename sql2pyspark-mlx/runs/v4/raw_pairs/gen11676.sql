SELECT ss.id, ss.units, ss.store_id, p.stock_qty, p.price, s.supplier_name, s.rating FROM store_sales ss LEFT JOIN products p ON ss.product_id = p.id INNER JOIN suppliers s ON p.supplier_id = s.id;
