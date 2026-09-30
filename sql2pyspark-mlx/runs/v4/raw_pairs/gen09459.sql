SELECT i.id, i.product_id
FROM inventory i
WHERE i.product_id IN (SELECT id FROM products WHERE brand IN ('Initech', 'Acme'))
