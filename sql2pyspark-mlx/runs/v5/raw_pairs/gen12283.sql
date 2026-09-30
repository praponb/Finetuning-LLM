SELECT SORT_ARRAY(COLLECT_SET(category)) AS category_list, SUM(CASE WHEN category = 'Electronics' THEN price ELSE 0 END) AS electronics_price, COUNT(*) AS row_count
FROM products
WHERE launch_date < '2025-02-13' AND stock_qty > 317
ORDER BY electronics_price DESC, row_count
