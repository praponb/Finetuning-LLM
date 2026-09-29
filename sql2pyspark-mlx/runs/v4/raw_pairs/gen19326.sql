SELECT
    i.last_restock,
    s.opened_date,
    s.store_name,
    s.city
FROM inventory i
LEFT OUTER JOIN stores s ON s.id = i.store_id
WHERE i.quantity < 204 AND s.city IN ('London', 'Berlin', 'Singapore')
ORDER BY store_name, opened_date;
