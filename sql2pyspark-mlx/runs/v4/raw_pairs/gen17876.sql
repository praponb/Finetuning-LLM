SELECT suppliers.id, suppliers.country FROM suppliers WHERE NOT EXISTS (SELECT 1 FROM products WHERE products.supplier_id = suppliers.id AND products.price <= 1100) ORDER BY country, id DESC
