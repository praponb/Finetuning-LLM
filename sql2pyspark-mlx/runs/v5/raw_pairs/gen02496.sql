SELECT
    s.city,
    COUNT(i.id) AS inventory_count,
    MIN(i.quantity) AS min_quantity,
    SUM(i.quantity) AS total_quantity
FROM stores s
LEFT JOIN inventory i ON i.store_id = s.id
GROUP BY s.city
ORDER BY min_quantity DESC, city
LIMIT 1;
