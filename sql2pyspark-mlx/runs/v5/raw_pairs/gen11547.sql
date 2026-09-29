SELECT
    i.id,
    i.quantity,
    i.store_id,
    p.cost,
    s.country
FROM inventory i
LEFT JOIN products p ON p.id = i.product_id
FULL OUTER JOIN suppliers s ON p.supplier_id = s.id
