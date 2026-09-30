SELECT MAX(launch_date) last_launch_date, SUM(CASE WHEN brand = 'Umbrella' THEN 1 ELSE 0 END) umbrella_count
FROM products
ORDER BY last_launch_date DESC;
