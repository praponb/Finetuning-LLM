SELECT a.id, a.product_id, a.last_restock, d.launch_date FROM inventory a LEFT JOIN products d ON a.product_id = d.id WHERE d.supplier_id IS NULL;
