SELECT
    s.id,
    s.store_name,
    COUNT(i.id) AS inventory_count,
    ROUND(AVG(i.quantity), 2) AS avg_quantity
FROM stores s
LEFT JOIN inventory i ON s.id = i.store_id
WHERE s.opened_date < '2025-01-01'
GROUP BY s.id, s.store_name;
