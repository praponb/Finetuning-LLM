SELECT id, supplier_id, CASE WHEN brand = 'Acme' THEN 1 ELSE 0 END is_acme
FROM products
ORDER BY id DESC;
