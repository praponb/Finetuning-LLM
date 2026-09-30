SELECT stores.id, stores.sqft, stores.store_name FROM stores WHERE EXISTS (SELECT 1 FROM store_sales WHERE store_sales.store_id = stores.id)
