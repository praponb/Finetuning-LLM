SELECT *, CASE WHEN region = 'East' THEN 1 ELSE 0 END AS is_east, REPLACE(store_name, ' ', '-') AS store_name_slug FROM stores WHERE city IN ('Tokyo', 'Berlin', 'Singapore') AND region = 'West';
