SELECT
    i.id,
    i.product_id,
    i.last_restock,
    p.cost,
    s.city
FROM inventory i
JOIN products p ON p.id = i.product_id
LEFT JOIN suppliers s ON p.supplier_id = s.id
ORDER BY cost
