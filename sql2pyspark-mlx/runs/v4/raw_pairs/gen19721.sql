SELECT
    i.id,
    i.store_id,
    s.region,
    s.store_name
FROM inventory i
INNER JOIN stores s ON s.id = i.store_id;
