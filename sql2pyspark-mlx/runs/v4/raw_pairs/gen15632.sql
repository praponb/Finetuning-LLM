SELECT
    p.id,
    p.product_name,
    p.launch_date,
    p.cost
FROM products p
WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE rating <= 3.7);
