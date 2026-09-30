SELECT CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(brand))) AS brand_values, MIN(launch_date) AS min_launch_date
FROM products
WHERE launch_date > '2024-06-01' AND category = 'Home';
