SELECT id, sqft, INITCAP(TRIM(city)) AS city_title, CONCAT_WS('-', region, store_name) AS region_store_name_key FROM stores WHERE store_name LIKE '%Rive%' ORDER BY city_title;
