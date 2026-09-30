SELECT
    p.id,
    p.supplier_id,
    p.price,
    p.product_name
FROM products p
WHERE p.id IN (SELECT product_id FROM inventory WHERE last_restock < '2024-12-01')
ORDER BY supplier_id
