SELECT
    i.id,
    i.last_restock,
    s.store_name,
    s.opened_date
FROM inventory i
LEFT JOIN stores s ON i.store_id = s.id
WHERE i.product_id = 2 OR s.sqft BETWEEN 7702 AND 11755
ORDER BY s.store_name, last_restock;
