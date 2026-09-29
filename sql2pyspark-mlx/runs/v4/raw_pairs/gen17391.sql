SELECT
    i.last_restock,
    i.quantity,
    p.brand,
    s.supplier_name,
    s.rating
FROM inventory i
LEFT OUTER JOIN products p ON i.product_id = p.id
INNER JOIN suppliers s ON s.id = p.supplier_id
WHERE MONTH(p.launch_date) IN (6, 8, 9) AND i.quantity BETWEEN 160 AND 172
