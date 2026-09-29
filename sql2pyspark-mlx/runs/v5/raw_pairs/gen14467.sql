SELECT id, region, LOWER(store_name) AS store_name_lower FROM stores WHERE sqft >= 15334 AND city IN ('Singapore', 'Berlin');
