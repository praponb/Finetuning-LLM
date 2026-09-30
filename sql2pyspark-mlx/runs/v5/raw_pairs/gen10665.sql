SELECT ss.store_id, ss.product_id, p.price, p.cost, s.city, s.supplier_name FROM store_sales ss LEFT JOIN products p ON ss.product_id = p.id LEFT JOIN suppliers s ON p.supplier_id = s.id;
