SELECT
    p.id,
    p.brand,
    p.category,
    p.launch_date
FROM products p
WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE city = 'Berlin')
ORDER BY id DESC
LIMIT 10;
