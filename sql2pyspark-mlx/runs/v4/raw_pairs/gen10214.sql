SELECT src.*, b.country, b.supplier_name FROM products src INNER JOIN suppliers b ON src.supplier_id = b.id
