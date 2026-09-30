SELECT suppliers.id, suppliers.city, suppliers.country
FROM suppliers
WHERE NOT EXISTS (SELECT 1 FROM products WHERE products.supplier_id = suppliers.id AND LENGTH(products.product_name) < 13)
