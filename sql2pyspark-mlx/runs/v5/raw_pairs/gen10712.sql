SELECT
    p.region,
    p.city,
    COUNT(*) AS rows_total,
    SUM(s.quantity) AS total_quantity
FROM inventory s
INNER JOIN stores p ON s.store_id = p.id
GROUP BY GROUPING SETS ((p.region, p.city), (p.region))
ORDER BY rows_total DESC;
