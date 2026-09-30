SELECT
    i.id,
    i.quantity,
    i.product_id,
    p.cost,
    p.category,
    s.city,
    s.region
FROM inventory i
FULL OUTER JOIN products p ON i.product_id = p.id
LEFT JOIN stores s ON i.store_id = s.id
WHERE NOT (i.store_id IN (1, 5, 6) AND i.product_id = 10);
