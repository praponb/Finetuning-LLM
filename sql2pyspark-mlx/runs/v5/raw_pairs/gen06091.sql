SELECT
    p.city,
    p.region,
    COUNT(*) AS rows_total,
    MIN(s.quantity) AS min_quantity,
    GROUPING_ID() AS grouping_level
FROM inventory s
INNER JOIN stores p ON s.store_id = p.id
GROUP BY GROUPING SETS ((p.city, p.region), (p.city));
