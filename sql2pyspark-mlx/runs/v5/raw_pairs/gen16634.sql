SELECT
    p.region,
    COUNT(*) AS cnt,
    AVG(src.quantity) AS avg_quantity
FROM inventory src
INNER JOIN stores p ON src.store_id = p.id
GROUP BY p.region;
