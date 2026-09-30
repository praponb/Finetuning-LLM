SELECT
    d.region,
    COUNT(*) AS cnt,
    MAX(s.quantity) AS max_quantity
FROM inventory s
JOIN stores d ON s.store_id = d.id
GROUP BY CUBE (d.region);
