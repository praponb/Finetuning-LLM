SELECT id, store_name, sqft, COALESCE(store_name, 'Unknown') AS store_name_filled, LOWER(city) AS city_lower FROM stores WHERE sqft BETWEEN 5740 AND 12416 ORDER BY city_lower, id LIMIT 10
