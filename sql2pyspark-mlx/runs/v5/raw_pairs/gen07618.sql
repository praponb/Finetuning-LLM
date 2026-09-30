SELECT
    i.id,
    i.last_restock,
    i.store_id,
    i.quantity,
    s.city,
    s.opened_date,
    s.store_name
FROM inventory i
JOIN stores s ON i.store_id = s.id
WHERE s.region = 'Central';
