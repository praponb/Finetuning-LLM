SELECT
    e.*,
    d.store_name,
    d.city
FROM inventory e
JOIN stores d
    ON e.store_id = d.id;
