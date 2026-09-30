SELECT id, opened_date, region, city, SUBSTRING(store_name, 1, 2) AS store_name_prefix FROM stores ORDER BY region, id LIMIT 20;
