SELECT p.id, p.supplier_id, p.product_name, s.rating, s.supplier_name FROM products p INNER JOIN suppliers s ON p.supplier_id = s.id;
