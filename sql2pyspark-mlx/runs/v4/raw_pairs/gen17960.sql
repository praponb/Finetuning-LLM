SELECT
    i.id,
    i.quantity,
    i.store_id,
    i.last_restock,
    s.region,
    s.store_name
FROM inventory i
JOIN stores s ON s.id = i.store_id
WHERE s.city = 'Singapore' OR s.region = 'South'
ORDER BY i.quantity DESC, i.last_restock
