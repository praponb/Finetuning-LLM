SELECT ss.store_id, ss.units, p.launch_date, p.supplier_id, p.product_name FROM store_sales ss LEFT JOIN products p ON ss.product_id = p.id WHERE p.supplier_id IS NOT NULL
