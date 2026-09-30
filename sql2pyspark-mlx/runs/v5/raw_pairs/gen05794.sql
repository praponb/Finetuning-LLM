SELECT
    p.id,
    p.price,
    p.supplier_id,
    p.category
FROM products p
WHERE p.supplier_id NOT IN (SELECT id FROM suppliers WHERE rating < 2.0)
ORDER BY supplier_id, id
LIMIT 20
