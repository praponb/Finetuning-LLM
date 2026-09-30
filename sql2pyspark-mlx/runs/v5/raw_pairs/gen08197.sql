SELECT *, CASE WHEN region = 'South' THEN 1 ELSE 0 END AS is_south, LENGTH(store_name) AS store_name_length FROM stores WHERE region = 'West' AND region = 'Central' ORDER BY region;
