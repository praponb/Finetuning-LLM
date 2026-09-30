SELECT
    i.id,
    i.quantity,
    i.store_id,
    p.launch_date,
    s.opened_date
FROM inventory i
FULL OUTER JOIN products p ON p.id = i.product_id
INNER JOIN stores s ON i.store_id = s.id
WHERE NOT (p.brand IN ('Globex', 'Umbrella', 'Stark') AND i.store_id IN (3, 5, 6))
ORDER BY launch_date ASC;
