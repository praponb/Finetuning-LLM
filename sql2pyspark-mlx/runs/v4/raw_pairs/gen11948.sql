SELECT suppliers.id, suppliers.rating, suppliers.city
FROM suppliers
WHERE NOT EXISTS (SELECT 1 FROM products WHERE products.supplier_id = suppliers.id AND products.cost > 410)
ORDER BY id DESC
