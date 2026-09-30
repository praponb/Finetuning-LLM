SELECT
    i.id,
    i.last_restock,
    i.product_id,
    s.store_name,
    s.city,
    s.opened_date
FROM inventory i
INNER JOIN stores s ON i.store_id = s.id
WHERE i.quantity > 149 AND s.sqft > 6468
ORDER BY s.city
