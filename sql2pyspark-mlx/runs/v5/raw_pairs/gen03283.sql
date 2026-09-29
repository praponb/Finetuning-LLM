SELECT p.city, COUNT(*) AS rows_total, ROUND(AVG(s.quantity), 2) AS avg_quantity FROM inventory s LEFT JOIN stores p ON s.store_id = p.id GROUP BY p.city ORDER BY city DESC;
