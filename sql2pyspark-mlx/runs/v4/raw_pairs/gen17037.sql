SELECT
    s.id,
    s.store_name,
    COUNT(i.id) AS inventory_count,
    COUNT(DISTINCT i.quantity) AS unique_products
FROM stores s
LEFT JOIN inventory i ON s.id = i.store_id
GROUP BY s.id, s.store_name
ORDER BY unique_products DESC
