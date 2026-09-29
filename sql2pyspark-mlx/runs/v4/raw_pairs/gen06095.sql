SELECT stores.id, stores.store_name, stores.sqft FROM stores WHERE NOT EXISTS (SELECT 1 FROM store_sales WHERE store_sales.store_id = stores.id);
