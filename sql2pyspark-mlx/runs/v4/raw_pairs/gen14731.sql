SELECT
    i.id,
    i.quantity,
    s.city,
    p.cost
FROM inventory i
INNER JOIN stores s ON i.store_id = s.id
LEFT JOIN products p ON i.product_id = p.id
WHERE s.city != 'New York';
