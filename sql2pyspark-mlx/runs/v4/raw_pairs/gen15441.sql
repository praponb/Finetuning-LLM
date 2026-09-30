SELECT ss.store_id, ss.revenue, p.supplier_id, s.opened_date, s.city FROM store_sales ss JOIN products p ON ss.product_id = p.id LEFT JOIN stores s ON ss.store_id = s.id;
