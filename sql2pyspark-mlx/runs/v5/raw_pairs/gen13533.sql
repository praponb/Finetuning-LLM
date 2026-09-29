SELECT products.id, products.category
FROM products
WHERE products.supplier_id IN (SELECT id FROM suppliers WHERE country = 'SG')
ORDER BY products.id DESC
LIMIT 5;
