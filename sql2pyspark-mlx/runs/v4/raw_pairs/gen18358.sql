SELECT supplier_id FROM products WHERE brand != 'Globex' UNION ALL SELECT supplier_id FROM products WHERE brand IN ('Globex', 'Initech', 'Acme')
