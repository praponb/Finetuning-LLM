SELECT YEAR(p.launch_date) launch_year, MIN(i.last_restock) min_last_restock
FROM inventory i
INNER JOIN products p ON p.id = i.product_id
WHERE i.store_id IN (1, 3)
GROUP BY YEAR(p.launch_date)
ORDER BY min_last_restock, launch_year
LIMIT 3;
