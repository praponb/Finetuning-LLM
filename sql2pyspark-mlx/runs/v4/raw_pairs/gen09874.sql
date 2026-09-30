SELECT id, sqft, SUBSTRING(store_name, 1, 2) AS store_name_prefix, TRIM(city) AS clean_city FROM stores;
