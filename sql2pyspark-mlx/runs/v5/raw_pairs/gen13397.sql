SELECT suppliers.id, suppliers.country, suppliers.supplier_name
FROM suppliers
WHERE suppliers.city = 'Berlin' AND NOT EXISTS (SELECT 1 FROM products WHERE products.supplier_id = suppliers.id AND products.product_name IS NOT NULL)
