SELECT i.last_restock, p.launch_date FROM inventory i INNER JOIN products p ON p.id = i.product_id WHERE i.store_id IN (2, 5) AND p.supplier_id = 14;
