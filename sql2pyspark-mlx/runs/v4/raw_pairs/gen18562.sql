SELECT
    i.id,
    i.quantity,
    i.last_restock,
    p.launch_date,
    s.country
FROM inventory i
INNER JOIN products p ON p.id = i.product_id
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE p.category = 'Fashion';
