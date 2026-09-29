SELECT id, supplier_id
FROM products
EXCEPT
SELECT id, supplier_id
FROM products
WHERE brand IN ('Umbrella', 'Acme', 'Initech');
