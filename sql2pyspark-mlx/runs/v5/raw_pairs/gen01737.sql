SELECT p.brand, COUNT(*) AS rows_total, ROUND(AVG(s.quantity), 2) AS avg_quantity FROM inventory s INNER JOIN products p ON s.product_id = p.id GROUP BY p.brand ORDER BY avg_quantity ASC NULLS LAST;
