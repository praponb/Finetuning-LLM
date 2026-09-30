SELECT stores.id, stores.city FROM stores WHERE NOT EXISTS (SELECT 1 FROM store_sales WHERE store_sales.store_id = stores.id AND YEAR(store_sales.sale_date) >= 2025) ORDER BY city;
