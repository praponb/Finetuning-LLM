SELECT stores.id, stores.region, stores.sqft FROM stores WHERE EXISTS (SELECT 1 FROM store_sales WHERE store_sales.store_id = stores.id AND store_sales.store_id IN (4, 6));
