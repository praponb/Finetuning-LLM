SELECT
    i.last_restock,
    i.quantity,
    s.store_name,
    s.region,
    s.city
FROM inventory i
INNER JOIN stores s ON i.store_id = s.id
WHERE i.quantity BETWEEN 77 AND 143;
