SELECT MAX(launch_date) AS latest_launch_date, COUNT(CASE WHEN category = 'Fashion' THEN 1 END) AS fashion_count, SUM(CASE WHEN category = 'Sports' THEN stock_qty ELSE 0 END) AS sports_stock_qty
FROM products
WHERE (cost > 410 AND ARRAY_CONTAINS(tags, 'premium')) OR supplier_id IS NOT NULL
ORDER BY sports_stock_qty DESC;
