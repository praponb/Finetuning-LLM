SELECT
    i.id,
    i.product_id,
    i.last_restock,
    p.price,
    p.cost,
    s.opened_date,
    s.sqft
FROM inventory i
LEFT JOIN products p ON i.product_id = p.id
INNER JOIN stores s ON i.store_id = s.id
WHERE s.region IN ('North', 'West', 'Central')
ORDER BY id DESC
