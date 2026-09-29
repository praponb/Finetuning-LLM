SELECT suppliers.id, suppliers.supplier_name FROM suppliers WHERE suppliers.id IN (SELECT supplier_id FROM products WHERE cost >= 390);
