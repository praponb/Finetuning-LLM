SELECT suppliers.id, suppliers.country
FROM suppliers
WHERE suppliers.id IN (SELECT supplier_id FROM products);
