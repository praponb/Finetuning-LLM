SELECT e.*, d.rating, d.supplier_name, d.city FROM products e INNER JOIN suppliers d ON e.supplier_id = d.id;
